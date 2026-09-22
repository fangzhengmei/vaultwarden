# Vaultwarden WebSocket 实时同步广播链路分析

本文基于当前仓库源码，沿 cipher / folder 变更从"事件形成"到"消息送达"的完整链路进行分析，
并讨论目标筛选、权限变化、心跳重连与通知降级之间的相互影响，最后评估并发变更、重复事件
与失联设备对客户端结果的影响。

## 1. 总体架构：两条平行的通知通道

Vaultwarden 的实时同步由两条通道组成，在 `src/api/notifications.rs` 的每个
`send_*_update` 方法中并列触发：

- **WebSocket 通道**：客户端连接 `/notifications/hub`（路由注册见 `src/main.rs:589`，
  由 `src/api/notifications.rs:46` 的 `routes()` 按 `enable_websocket` 配置决定是否挂载）。
  服务端用全局单例 `WS_USERS`（`notifications.rs:27`）维护
  `DashMap<user_id, Vec<(entry_uuid, mpsc::Sender<Message>)>>`，即"用户 → 该用户所有在线连接的发送端"。
- **Push 通道**：`src/api/push.rs` 把同样的 UpdateType 通过 Bitwarden 官方 push relay
  转发给移动端（`push_enabled` 默认关闭，见 `src/config.rs:529`）。

每个 `send_*_update` 入口都有短路判断（`notifications.rs:44`）：

```rust
static NOTIFICATIONS_DISABLED: LazyLock<bool> = LazyLock::new(|| !CONFIG.enable_websocket() && !CONFIG.push_enabled());
```

两者都关时整个通知路径零开销；只开其一时另一通道静默跳过。这决定了"降级"的粒度是
**通道级**而非消息级：不存在"WS 发送失败自动转 push"的逻辑。

## 2. 事件形成与发送链路

### 2.1 Cipher 变更链路

以更新 cipher 为例（`src/api/core/ciphers.rs` 的 `update_cipher_from_data`，约 597 行）：

1. **写库**：`cipher.save(conn)`（`src/db/models/cipher.rs:438`）。注意 `save` 内部
   先调 `update_users_revision` 再把 `updated_at` 设为当前时间——revision 的 bump
   发生在行数据落库之前。
2. **计算目标**：handler 再次调用 `cipher.update_users_revision(conn)`
   （`cipher.rs:410`），返回 `Vec<UserId>`：
   - 个人 cipher：仅属主本人；
   - 组织 cipher：`Membership::find_by_cipher_and_org`（`src/db/models/organization.rs:1064`）
     选出 `access_all` 成员 ∪ 通过 `users_collections → ciphers_collections` 关联到该
     cipher 的成员；启用 groups 时再并入 `find_by_cipher_and_org_with_group`
     （`organization.rs:1086`）的组成员。筛选条件是**发送那一刻的数据库 ACL 状态**，
     不是事件订阅关系。
3. **构造消息**：`send_cipher_update`（`notifications.rs:430`）调用 `create_update`
   （`notifications.rs:627`）生成 SignalR MessagePack 二进制帧：
   `[1, {}, nil, "ReceiveMessage", [{ContextId, Type, Payload}]]`。
   - `ContextId` = 发起操作的 device uuid，供客户端做回声抑制（自己改的不必再全量拉取）；
   - `Type` = `UpdateType` 枚举（`notifications.rs:672`，与上游 PushType 对齐）；
   - `Payload` 携带 `Id / UserId / OrganizationId / CollectionIds / RevisionDate`。
   集合归属变化时（`post_collections` 等）`UserId` 置 null、`RevisionDate` 取当前时间，
   否则客户端不会触发集合同步（`notifications.rs:444-454` 的注释明确说明）。
4. **投递**：`send_update`（`notifications.rs:354`）先 clone 出发送端列表（避免持
   DashMap 锁跨 await），再对每个连接 `sender.send(Message::binary(data)).await`。
   发送失败只记 error 日志，不重试、不剔除连接（清理依赖连接断开时 guard 的 Drop）。

### 2.2 Folder 变更链路

Folder 更简单（`src/api/core/folders.rs`）：`folder.save/delete`
（`src/db/models/folder.rs:75,101`）内部只 bump 属主 revision，随后
`send_folder_update`（`notifications.rs:406`）向 `folder.user_uuid` 这一个用户广播
`SyncFolderCreate/Update/Delete`。Folder 无组织概念，目标筛选退化为单用户。

### 2.3 连接生命周期

`websockets_hub`（`notifications.rs:123`）握手时校验 access token（query 或 header），
为每条连接分配 `entry_uuid` 与容量 100 的 mpsc channel，注册进 `WS_USERS`；
`WSEntryMapGuard` 的 Drop（`notifications.rs:78`）在连接结束时把自己从 Vec 中移除。
连接循环用 `tokio::select!` 同时处理三类事件：客户端消息（含 SignalR 握手
`{protocol:"messagepack",version:1}` 与 ping/pong）、`rx.recv()` 转发服务端消息、
15 秒定时器发送 SignalR ping（`serialize([6])`，`notifications.rs:666`）。

## 3. 目标筛选、权限变化、心跳重连、降级之间的相互影响

### 3.1 目标筛选与权限变化

- **筛选是"拉取式"的**：每次事件都现算 `update_users_revision`，因此权限收窄立即生效——
  用户被移出集合后，该 cipher 的后续更新不会再发给他。这是正确性上理想的行为。
- **但权限变化本身几乎不产生通知**：`organizations.rs` 中只有成员确认（1478 行）与
  成员移除（1739 行）发送 `SyncOrgKeys`；集合的增删、`CollectionUser` 的变更只在模型层
  `update_users_revision`（`src/db/models/collection.rs:213,939`）bump 了用户的
  `updated_at`，**没有任何 WS/push 消息**。也就是说"你被移出某集合"这件事，客户端只能在
  下一次由其他事件触发的同步（或手动同步、重连后的全量 sync）中感知。目标筛选的即时性
  与权限事件的惰性传播之间存在一个可见性空窗：被移除者在此期间的本地副本仍显示旧数据，
  只是不再收到新更新。
- **硬删除的时序缺陷**：`delete_cipher_by_uuid`（`ciphers.rs:1837` 起）先
  `cipher.delete()`（内部已级联删除 `CollectionCipher` 关联），**之后**才调
  `update_users_revision` 计算通知目标。此时集合关联已不存在，
  `find_by_cipher_and_org` 只剩 `access_all` 分支能命中——组织 cipher 被硬删除时，
  仅通过集合授权的普通成员收不到 `SyncLoginDelete`，他们的客户端会保留这条已删除的
  cipher 直到下次全量同步。
- **批量操作的广播坍缩**：multi-delete / multi-restore / multi-move 抑制了逐条
  `send_cipher_update`，最后只给**操作者本人**发 `SyncCiphers`/`SyncVault`
  （`ciphers.rs:1891,1959,1674`）。对个人保险库这足够，但组织 cipher 的批量删除/
  恢复对其他组织成员完全不产生任何通知，同样依赖全量同步兜底。

### 3.2 心跳与重连

- 服务端每 15 秒发 SignalR ping，并对客户端 ping 回 pong，但**不校验客户端 pong 是否
  按时返回**，没有应用层超时。死连接依赖 TCP/WebSocket 层的关闭帧或读写错误
  （`ws.next()` 返回 `None`/`Err` 时 break）来发现。半开连接可能长期占用
  `WS_USERS` 里的 sender 槽位。
- access token 只在握手时 `decode_login` 校验一次（`notifications.rs:139`），连接存续
  期间 token 过期、用户被登出都不会主动断开已有 WS——权限收窄对"已建立连接"同样滞后。
- **没有断线重放机制**：消息发进 mpsc 即视为送达，连接断开期间的事件直接丢失。客户端
  的一致性完全依赖重连后用 `revision_date` 做增量/全量 sync，WS 消息只是"该同步了"
  的提示。这也是 `update_users_revision` 同时承担"bump revision"与"计算收件人"双重
  职责的原因——即使通知丢了，revision 已落库，全量 sync 能发现差异。
- 匿名 hub（`/anonymous-hub`，用于"用设备登录"审批）按 IP 限 25 条连接
  （`notifications.rs:42,566`），且同一 token 允许并存多个订阅者
  （`notifications.rs:218-222` 的注释：重连时旧连接不能把新连接顶掉），这是对
  重连场景的专门加固，但认证 hub 没有等价的每用户连接数上限。

### 3.3 通知降级

- **通道级降级**：`enable_websocket=false` 时路由不挂载并打 warn 日志
  （`notifications.rs:50`），实时同步只剩 push；反之亦然。两者互不知情、互不补位。
- **push 通道自身的降级点**（`src/api/push.rs`）：
  - 组织 cipher 不发 push（`push.rs:159`，与上游行为一致）——移动端组织成员只能
    靠 WS 或下次打开 App 时同步；
  - `send_cipher_update` 仅当 `user_ids.len() == 1` 才发 push（`notifications.rs:474`），
    多收件人场景 push 直接跳过；
  - relay 请求失败只记日志不重试（`push.rs:288` 起），token 获取失败同样静默；
  - `check_user_has_push_device`（`src/db/models/device.rs:252`）只检查
    `push_token` 非空，token 是否已被系统侧吊销无从得知；
  - 部分 push 用 `tokio::task::spawn` 发出（如 `push_logout`），与 HTTP 响应、与 WS
    消息之间无顺序保证。
- **背压与队头阻塞**：每条连接的 mpsc 容量为 100，`send_update` 使用
  `sender.send(...).await`。若某客户端停止读取（假死、网络挂起），其 channel 积满后，
  **执行广播的 API handler 任务会被异步阻塞**，直到该连接被 TCP 超时清理。一个失联
  设备可以拖慢对该用户（甚至组织广播循环中后续用户）的通知延迟——降级不是隔离的，
  慢消费者会反向影响生产者。

## 4. 并发变更、重复事件与失联设备对客户端结果的影响

### 4.1 并发变更

- 服务端没有全局事件序列号，也不保证跨连接、跨用户的送达顺序；仅单连接内 mpsc 保序。
  两个设备并发改同一 cipher 时，数据库层面后写覆盖先写（`updated_at = now`），
  通知层面两条 `SyncCipherUpdate` 的到达顺序与落库顺序可能不一致。
- 客户端的裁决依据是 `RevisionDate`：较旧的消息到达时应被丢弃。由于 vaultwarden
  的 RevisionDate 直接取行的 `updated_at`，与落库内容同源，最终状态收敛于最后写入，
  通知乱序不改变最终一致性，只可能造成客户端一次多余的 sync 请求。
- 并发广播共享 `DashMap` 分片锁，且 `send_update` 先 clone 再 await，避免了持锁
  跨 await 的死锁；代价是消息可能发给一个刚断开但尚未被 guard 清理的连接（无害，
  发送失败仅记日志）。

### 4.2 重复事件

- **同一变更多次 bump revision**：`cipher.save` 内部调一次 `update_users_revision`，
  handler 为取收件人又调一次，用户 `updated_at` 被刷新两次（值接近，无害但冗余）。
- **同一变更产生多条消息**：创建 cipher 时 `update_cipher_from_data` 之外还有
  `move_to_folder`、`set_favorite` 等附带写库；批量分享用 `UpdateType::None`
  （`ciphers.rs:1048`）显式抑制逐条通知再补一条 `SyncCiphers`，说明代码已意识到
  重复风暴问题，但单条路径上"cipher 更新 + folder 更新"等组合仍会让客户端收到多个
  指向同一 revision 的提示。
- 客户端侧这些重复是幂等的：消息本体不含数据，只触发"按 revision 重新拉取"，重复
  消息最多造成重复 HTTP sync，不会导致状态错误。真正的成本是组织 cipher 场景下
  `send_cipher_update` 对每个收件人串行 await（`notifications.rs:469-471`），
  大组织 + 批量操作时通知延迟被放大。

### 4.3 失联设备

- **WS 视角**：失联分两类。被及时发现的断线由 guard 清理，后续消息不再投递；未被
  发现的半开连接会持续接收消息进 mpsc，积满 100 条后反过来阻塞广播者（见 3.3）。
  对该设备本身，断线期间的事件全部丢失，恢复依赖重连后的全量 sync——由于
  `update_users_revision` 已把 revision 落库，sync 一定能发现差异，**丢消息不丢
  一致性**。
- **push 视角**：push token 失效的设备对服务端不可见，relay 投递失败静默吞掉；
  移动端在 App 被杀期间错过的 push 同样依赖下次启动时的 sync 兜底。
- **综合结论**：vaultwarden 的通知系统是"尽力而为的提示层 + revision 兜底"模型。
  失联设备的最坏结果不是数据错误，而是：(a) 该设备恢复前界面陈旧；(b) 半开连接
  积满 channel 时拖累同用户/同组织其他设备的通知实时性；(c) 在 3.1 所述的权限
  收窄与硬删除场景下，陈旧数据会一直保留到下一次全量同步。

## 5. 结论

整条链路的设计取舍是清晰的：消息体不含业务数据、服务端不存离线队列、筛选按发送时
ACL 现算、一致性完全交给 `revision_date` + 全量 sync。这让实现简单且不会出现"通知
内容泄露给已失权用户"的问题，但也带来三个结构性后果：

1. **可见性空窗**：权限变化（集合成员调整、组织 cipher 硬删除、批量删除/恢复）不产生
   或只产生部分通知，受影响客户端在下次全量同步前保持陈旧视图；
2. **故障耦合**：单连接 mpsc 背压使失联/假死设备能阻塞广播路径，通道级降级意味着
   任一通道故障都无补位；
3. **最终一致但延迟不定**：并发与重复消息对客户端无害（幂等提示 + RevisionDate 裁决），
   但"实时"的保证强度完全取决于 WS 连接健康度与客户端重连后的 sync 行为。
