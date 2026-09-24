# Vaultwarden 实时同步广播链路分析

> 分析对象：cipher / folder 变更 → 事件构造 → WebSocket / Push 广播的完整链路。
> 核心源码：`src/api/notifications.rs`、`src/api/push.rs`、`src/api/core/ciphers.rs`、
> `src/api/core/folders.rs`、`src/db/models/cipher.rs`、`src/db/models/device.rs`。

## 1. 事件形成与发送链路

### 1.1 总览

一次 cipher/folder 变更的广播链路为：

1. **HTTP 路由**（如 `put_folder`、`update_cipher_from_data`）先完成数据库写入
   （`folder.save()` / `cipher.save()`），再调用 `nt.send_*_update(...)`。
2. `WebSocketUsers::send_*_update`（notifications.rs）先检查
   `NOTIFICATIONS_DISABLED`（websocket 与 push 同时关闭时直接短路返回），
   然后用 `create_update()` 把负载编码为 **SignalR MessagePack 二进制帧**
   （`[1, {}, null, "ReceiveMessage", [{ContextId, Type, Payload}]]`）。
3. 若 `enable_websocket`：通过 `send_update()` 向目标用户在
   `WS_USERS`（DashMap：user_uuid → Vec<(entry_uuid, mpsc::Sender)>）中登记的
   **每一条连接** 的 mpsc channel（容量 100）发送同一份字节。
4. 若 `push_enabled`：再调用 `push_*_update`，向 Bitwarden 官方 push relay
   发 HTTP POST（`/push/send`），由 relay 经 FCM/APNs 触达移动设备。

WS 与 push 是**并行互补**的两条通道：桌面/浏览器靠 WS，移动端（Android/iOS，
`Device::is_push_device()`）靠 push relay。二者互不替代，失败互不影响。

### 1.2 负载构造的关键细节

`create_update()` 中的 `ContextId` 是**发起变更的设备 uuid**（acting device）。
客户端用它识别"这条通知是我自己触发的"，从而跳过本地已应用过的变更。
folder 与 cipher 通知都带 `ContextId`；`SyncVault`/`SyncCiphers` 这类
"整体重同步"通知（`send_user_update`）则不带（push 侧会带 `deviceId` 让 relay 跳过本机）。

cipher 通知的负载分两种形态（notifications.rs `send_cipher_update`）：

- **普通变更**：`UserId = cipher.user_uuid`，`RevisionDate = cipher.updated_at`，
  `CollectionIds = null`；
- **集合（collection）变更**（`collection_uuids.is_some()`，来自
  `post_collections_update` / share）：`UserId = null`、
  `RevisionDate = Utc::now()`、`CollectionIds = [...]`。
  注释明确说明：必须这样填，**否则客户端不会同步集合变化**——客户端对集合变更
  依赖新时间戳和集合列表来触发局部刷新。

### 1.3 降级（fallback）策略

- **批量操作降级为整体同步**：批量删除/恢复/归档/移动（`delete_multiple_ciphers`、
  `restore_multiple_ciphers`、`move_cipher_selected` 等）不给每条 cipher 发通知，
  而是最后发一条 `SyncCiphers`（或 `SyncVault`）让客户端全量重拉。
  导入（import）同理，逐条用 `UpdateType::None` 静默写入，最后发一条 `SyncVault`。
- **push 降级**：`send_to_push_relay` 失败（取 token 失败、HTTP 错误）只记
  `error!`/`debug!` 日志，不重试、不反馈给调用方——**best-effort**。
  push token 缓存有效期取上游返回值的**一半**，避免边界过期。
- **WS 发送失败**：`send_update()` 中 channel 发送失败仅 `error!` 日志，
  该条通知对此连接**永久丢失**（见 §4 失联设备）。
- **组织 cipher 无 push**：`push_cipher_update` 开头对
  `cipher.organization_uuid.is_some()` 直接 return（与上游行为一致）；
  且 `send_cipher_update` 中 push 仅在 `user_ids.len() == 1` 时触发。
  结果：**组织内移动端设备收不到实时推送**，只能等 App 前台同步。

## 2. 目标筛选：通知发给谁

### 2.1 个人对象

- folder：`send_folder_update` 只发给 `folder.user_uuid` 一个用户
  （folder 纯属个人概念，无共享）。
- 个人 cipher：`Cipher::update_users_revision()` 返回 `[cipher.user_uuid]`。

### 2.2 组织 cipher：按"当前权限快照"筛选

组织 cipher 的目标列表由 `Cipher::update_users_revision()`
（src/db/models/cipher.rs:410）在**发送通知的当下**实时计算：

- `Membership::find_by_cipher_and_org`：org 内 `access_all` 成员，
  或经 `users_collections → ciphers_collections` 关联到该 cipher 的成员；
- 若启用 groups，再叠加 `find_by_cipher_and_org_with_group`
  （经 `groups_users → collections_groups` 关联的成员）。

注意两个实现细节：

1. 两个查询各自 `distinct()`，但 `collection_users.extend(group_users)` 是
   **Vec 拼接、不去重**——同一用户同时被直接授权和组授权时会在列表里出现两次，
   导致该用户所有 WS 连接收到**两条完全相同的通知**（客户端按 cipher id 幂等处理，
   无害但浪费）。
2. 该函数同时负责**更新每个目标用户的 revision**（`User::update_uuid_revision`），
   而 `cipher.save()` 内部也会调一次，所以一次更新实际触发两轮 revision bump
   （幂等，只是多写一次 DB）。

### 2.3 权限变化与目标筛选的相互作用

这是整条链路里最容易产生可见性偏差的环节：

- **通知目标 = 变更后的权限快照**。集合变更（`post_collections_update`）先改
  `CollectionCipher` 关联，再算 `update_users_revision()`。于是：
  - **新获得访问权的成员**会收到 cipher 更新通知（负载里带新 CollectionIds），
    客户端能拉到该 cipher——传播正确；
  - **刚被移出集合/被降权的成员不在目标列表里**，收不到任何通知。
    他们本地的 cipher 副本会**残留**，直到下一次全量 sync / 重新登录才被清掉。
    也就是说"撤销可见性"这一事件本身**不传播**。
- **硬删除的时序问题**：`delete_cipher_by_uuid`（HardSingle）先调
  `cipher.delete()`——它内部先 `update_users_revision()`（此刻关联还在，
  只 bump revision），然后**删掉 `CollectionCipher` 关联**；之后才调用
  `send_cipher_update(SyncLoginDelete, ..., &cipher.update_users_revision())`。
  第二次计算目标列表时关联已空，**只剩 access_all 成员**会收到删除通知。
  普通集合成员收不到 `SyncLoginDelete`，本地保留墓碑数据直到全量同步。
  （软删除无此问题，因为关联未删。）
- **个人 → 组织的转移（share）**：`update_cipher_from_data` 中
  `cipher.user_uuid = None` 后才算目标列表，原属主只要仍在新集合的访问范围内
  就会以"组织成员"身份收到通知；若不在，则同样静默失去可见性。
- **账号级权限变化**走另一条路：改密码/换邮箱/重置安全戳（accounts.rs）会
  `send_logout`（带 acting device 的 `ContextId`，让发起设备自己豁免），
  强制其他设备登出重登——用"踢下线"代替"增量同步"，是最彻底的权限传播方式；
  组织密钥相关变化发 `SyncOrgKeys` 让客户端重取密钥。

## 3. 心跳、重连与连接生命周期

### 3.1 连接登记与清理

- `/notifications/hub` 握手时用 access token（query 或 header）换 `claims.sub`，
  生成随机 `entry_uuid`，把 `(entry_uuid, mpsc::Sender)` push 进该用户的 Vec。
  **同一用户可有多条连接**（多设备/多标签页），广播时逐条发送。
- `WSEntryMapGuard` 的 `Drop` 负责在流结束时从 map 中移除本连接——
  清理是**被动的**，依赖连接循环退出。
- 认证只在握手时发生：token 之后过期、被登出、设备被删除，**已建立的 WS 连接
  不会被主动断开**，仍会收到后续通知（客户端拿到通知后的 API 请求才会 401）。

### 3.2 心跳

- 服务端每 **15 秒**发一个 SignalR ping（MessagePack `[6]`），并应答客户端的
  WS 层 Ping/Pong。服务端**不校验客户端是否回 pong**——心跳的作用是保活
  （防 NAT/代理空闲断连），而不是探死。
- 探死依赖发送路径：向已断开的 channel 发送失败时记日志，连接条目要等
  `ws.next()` 返回错误/None 退出循环后才由 guard 移除。也就是说一条"半死"
  连接在 TCP 超时前会一直留在广播列表里，期间发给它的通知**静默丢失**。

### 3.3 重连

- 重连是**纯客户端行为**，服务端无会话/无序号/无补发。客户端重连后拿到的是
  一个全新的 `entry_uuid`，断线期间错过的通知**不会重放**，客户端必须靠
  断线检测后的全量 sync 补齐（Bitwarden 客户端的标准行为）。
- 旧连接若未立即消亡（TCP 半开），新旧两条连接会**同时在线**，同一条通知被
  投递两次——客户端按 cipher id + revisionDate 幂等去重。
- 匿名 hub（`/anonymous-hub`，无密码登录的 AuthRequest 场景）行为不同：
  注释明确说明**重连时保留所有旧订阅**（同 token 的 sender 不替换），
  因为客户端在登录请求 pending 期间会用同一 token 重连；代价是旧连接存活期间
  `AuthRequestResponse` 会重复投递。匿名 hub 另有每 IP 25 条连接的上限
  （`MAX_ANONYMOUS_CONNECTIONS_PER_IP`），超限返回 429。

## 4. 并发变更、重复事件与失联设备的影响判定

### 4.1 并发变更

- **无全局排序**：通知由各自 HTTP 处理任务独立发送，两个并发更新同一 cipher 的
  请求，其 WS 消息到达客户端的顺序**不保证与 DB 提交顺序一致**。
  客户端靠负载里的 `RevisionDate` 做最后写入胜出（LWW）判断，乱序到达时
  旧消息会被丢弃，最终状态收敛——**前提是客户端正确比较 revisionDate**。
- **写后读一致性**：通知在 `save()` 之后发送，且负载只含 id + revisionDate，
  客户端收到通知后回源拉取，拿到的一定是**不旧于**通知时刻的数据
  （可能更新——并发下拉到的是最新版，天然自愈）。
- **过期写保护**：`update_cipher_from_data` 校验 `last_known_revision_date`，
  客户端基于陈旧副本的写入会被拒（"Resync the client and try again"），
  防止并发覆盖。
- **目标列表竞态**：权限变更与 cipher 变更并发时，目标列表按各自时刻的快照
  计算，可能出现"某用户收到通知但已无权限拉取"（API 层 403，客户端忽略）
  或"没收到通知但已有权限"（下次全量同步补上）——两种方向都**安全降级**。

### 4.2 重复事件

重复来源有三：(a) 组+直接授权导致 `user_ids` 未去重（§2.2）；
(b) 重连期间新旧连接并存（§3.3）；(c) 匿名 hub 的同 token 多订阅。
所有重复都是**同内容重复**，客户端按 id 幂等处理，结果为无害的冗余流量。

### 4.3 失联设备

- **WS 侧**：通知即发即弃，无队列、无重放。设备断线期间的所有变更，
  服务端只保证 `users.revision` 已 bump——客户端重连后通过 sync 接口
  比较 revision 发现落后并全量补齐。**最终一致，但实时性完全依赖客户端
  的重连+重同步逻辑**。
- **channel 溢出**：单连接 mpsc 容量 100。若客户端连接活着但不消费
  （极端卡顿），第 101 条起通知被丢弃且仅记日志——与断线等效。
- **push 侧**：relay 发送失败不重试；移动端若同时没有 WS（组织 cipher 本来就
  不发 push），则完全静默，等下次前台同步。
- **设备删除/登出**：`unregister_push_device` 从 relay 注销 push_uuid；
  但 WS 连接不被主动关闭（§3.1），已登出设备的 WS 仍会收到通知直到连接自然
  终止——通知本身不含敏感密文（只有 id 和时间戳），风险可控。

## 5. 结论

整条链路的设计取向是 **"通知是提示，同步靠拉取"**：

1. 通知负载刻意不含数据（仅 id + revisionDate + 集合列表），任何丢失、重复、
   乱序都可由客户端回源拉取自愈；
2. 目标筛选按变更后的权限快照计算，"获得权限"能实时传播，"失去权限"不传播，
   依赖全量同步收敛——这是传播一致性与实现简洁性之间的取舍；
3. 心跳只保活不探死，重连无补发，失联设备的可见性完全由
   `users.revision` + 客户端全量 sync 兜底；
4. 通知降级链条为：单条实时通知 → 批量操作的整体 sync 通知 →
   （push 失败/WS 断开时）下次主动同步。任何一级失效都不会破坏最终一致性，
   只会拉长变更对其他设备可见的延迟。

