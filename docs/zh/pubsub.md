# Google Cloud Pub/Sub

`ruststream-gcp-pubsub` 在 Google Cloud Pub/Sub 上运行 RustStream 服务，底层是官方的
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) 客户端。Pub/Sub 把主题和订阅分开：
服务向主题发布，处理器消费订阅，订阅替它把该主题的消息排好队。框架本身的概念（编写订阅者、路由、
编解码器和中间件）在 [RustStream 文档](https://powersemmi.github.io/ruststream/)里。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

## 能力 { #capabilities }

框架的可选能力在 Pub/Sub 上的情况：

| 能力 | 原生 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 你按[订阅名](#subscriptions)订阅 |
| `BatchSubscriber` | 客户端 | 切片处理器拿到[在客户端攒出的批次](#batches) |
| `TransactionalPublisher` | 否 | 把一串消息归到一起的是[排序键](#ordering-keys) |
| `OwnedTransactions` | 否 | 这里没有可供拥有的发布事务 |
| `RequestReply` | 否 | 你用一个回复主题加一个关联属性自己搭 |
| `Partitioned` | 是 | [分区键就是消息的排序键](#ordering-keys) |
| `Seekable` 和 `Positioned` | 否 | 整条订阅的位置由管理端的 `seek` 移动，移到某个时间点或某个快照 |
| `DescribeServer` | 是 | 以 `googlepubsub` 协议报告正在使用的主机和端口（模拟器、自定义服务地址或 `pubsub.googleapis.com`）；写进服务地址里的 scheme、路径和凭据不会进入文档 |

处理器用 `message.partition_key()` 从投递里读排序键，为此不需要从本 crate 导入任何东西。

## 生命周期 { #the-lifecycle }

Broker 的每个状态都是一个独立的类型，走到下一个状态的转换会消费掉上一个：

```text
PubSubBroker::new(project)   只记配置，同步，无 I/O
  .connect()   ->  ConnectedPubSubBroker    活的客户端；订阅和发布者
  .shutdown()  ->  ()                       冲刷掉每一批缓冲的发布
```

`shutdown` 消费掉已连接的 Broker，所以它之后的发布或订阅不会通过编译。更早交出去的发布者不在这条
保证之内：连接一旦没了，它返回 `PubSubError::NotConnected`，而不是收下一条没人会发出去的消息。

Broker 默认用 Application Default Credentials 认证。`credentials(..)` 改用你自己的
`google_cloud_auth::credentials::Credentials`。`endpoint(..)` 指定另一个服务地址，区域地址也在
其中。`emulator(host)` 把每个客户端都指向本地[模拟器](#the-emulator)。

## 订阅 { #subscriptions }

`GooglePubSub` 说明处理器消费哪条订阅，可以用短 id，也可以用完整的
`projects/{project}/subscriptions/{name}` 资源名。它写在 `#[subscriber(..)]` 属性里：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:handler"
```

挂载点把处理器和 Broker 配到一起：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

描述符有四个选项：

- `create_with_topic(topic)` 在订阅时创建尚不存在的订阅和它的主题。它面向本地开发和针对
  [模拟器](#the-emulator)的测试；生产环境的订阅通常按基础设施来管理。
- `max_outstanding(n)` 是流控：同时可以有多少条已投递的消息处于未确认状态。这才是真正的预取，
  默认值是客户端的 1000。
- `ack_extension(duration)` 是处理器运行期间，每次后台续期把确认截止时间推多远。客户端会把它夹到
  协议的 10s 到 600s 区间，默认 60s。
- `batch_wait(duration)` 是一个未满的[批次](#batches)等待后续投递的时长，默认 50ms。

订阅名或主题名为空的描述符，在任何 I/O 之前就返回 `PubSubError::InvalidDescriptor`。

订阅用流式拉取消费。处理器运行期间，客户端在后台续期确认截止时间，因此处理器慢本身不会引发重新
投递。丢弃订阅者会把这条流排空。

`#[subscriber("orders-workers")]` 这种纯字符串写法，指的是同一个描述符加它的默认值，所以订阅必须
事先存在。

一条订阅只由名字标识，因此名字可以由挂载点给出：处理器上写 `#[subscriber(GooglePubSub)]`，挂载处
写 `.name("orders-workers")`。一个处理器服务于两套只有订阅名不同的部署，靠的就是这个。

## 批次 { #batches }

接收切片的处理器拿到的是整个批次：一次数据库往返、一次批量 API 调用，按批算而不是按订单算。

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_batches.rs:handler"
```

批次大小由挂载点给出，不给出大小的批量处理器挂载不上：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_batches.rs:app"
```

流式拉取一次交出一条投递，所以批次是在客户端攒出来的。一个批次装的消息绝不会超过挂载点说的大小，
也可能更少。

大小是批次的一半，描述符上的 `batch_wait` 是另一半，它 50ms 的默认值大约是一轮流式拉取的时长。
再短，多数批次会在第一条投递上就结束；再长，安静的订阅会白白扣住一个批次。批次值回一次网络往返
时，就把它调大。

`max_outstanding` 限制的是拉取，不是批次。

`PubSubTestBroker` 用同样的方式攒批次，所以切片处理器可以[在进程内测试](#testing)，而不只是对着
模拟器测。

## 确认 { #acknowledgement }

确认是原生的，而且逐条进行：

| 处理器结果 | Pub/Sub 调用 | 效果 |
| --- | --- | --- |
| `HandlerOutcome::ack()` | acknowledge | 这次投递完成 |
| `HandlerOutcome::retry()` | nack | 消息重新变为可取，会再投递一次 |
| `HandlerOutcome::drop()` | acknowledge | 消息不会重新投递 |

Pub/Sub 没有“丢弃且不重新投递”这个动作，所以 `drop()` 走的是确认。poison 消息由订阅的死信策略
分流，该策略设在订阅资源上。在这样的策略下，投递尝试次数会以 `pubsub-delivery-attempt` 消息头送
达（导出为 `DELIVERY_ATTEMPT_HEADER`），处理器可以按一条消息回来过几次来分支。

Pub/Sub 没有延迟 nack，所以 `HandlerOutcome::retry_after(delay)` 走运行时的
[延后重新发布](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#delayed-redelivery)：
用 `retry_via` 在作用域上接一个发布者，它取自 `b.broker().publisher()`。运行时随后确认这次投递，
并在延迟之后发布消息的一个副本。没有这个发布者，延迟就丢掉了，消息立刻重新入队，运行时给出告警。

副本进的是订阅所绑定的那个主题，绝不是订阅名：在 Pub/Sub 上，一次发布寻址的是主题。
`GooglePubSub` 报告的正是这个主题：`create_with_topic` 指定的那个，或者对按基础设施管理的订阅，
API 报告的那个，启动时问一次。用纯字符串声明的处理器报告不出主题，因为订阅名到不了任何地方，
所以在 `#[subscriber("orders-workers")]` 上接了 `retry_via` 的作用域会拒绝启动，并点名这条订阅。
改用 `GooglePubSub::new("orders-workers")` 声明该处理器就修好了。

### 精确一次确认 { #exactly-once-acknowledgement }

精确一次投递是订阅资源上的一个设置，所以同一份处理器代码在两种订阅上都能跑。在精确一次订阅上，
只有服务确认之后 `ack` 才返回 `Ok`，此后这条消息不会重新投递。遭到拒绝的确认（过期的 ack id、
输掉的截止时间竞争）返回 `AckError::Broker`，而不是当成功放过去。在普通订阅上，确认一入队 `ack`
就返回 `Ok`。

## 排序键 { #ordering-keys }

共享一个排序键的消息，会按发布顺序投递给同一个订阅者。这个键是消息的一个字段，不是消息头，而且它
是 Pub/Sub 的一次发布在载荷和属性之外唯一的设置。`PubSubPublishOptions` 就是这项设置的类型形态，
`ordering_key` 是它唯一的字段。

键从两个地方进入一次发布。挂载点为整个槽位固定它，属于同一个实体的槽位要的正是这个：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:ordered"
```

单次发布用 `ordering_key` 步骤说出自己的键，位置在 `message(..)` 和 `publish()` 之间。该步骤是
框架发布构建器上的一个位置，所以由它收尾的发布仍然保留挂载点定下的其余一切：那个条目指定的
编解码器、它的变换，以及测试断言所依据的槽位。

点名这个步骤的处理器主体会导入本 crate 的 prelude，并在签名里写明这一点，这是主体唯一一次点名
自己运行在哪个 Broker 上：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_ordered_publish.rs:handler"
```

不点名键的主体两样都不需要，它按挂载点固定的键发送。

回复没有调用点，所以它的键来自策略：`.out(Reply, Publish::default().ordering_key("receipts"))`。
每条回复各不相同的键，交给挂载链上的 `.transform(..)`：它读取投递，写入回复的 `partition-key`
消息头。

在投递这一侧，框架的分区键*就是*排序键：一次投递通过 `message.partition_key()` 报告它，也在
`partition-key` 消息头下报告它（导出为 `PARTITION_KEY_HEADER`），因此读取键的处理器不需要从本
crate 导入任何东西。对不针对具体 Broker 编写的服务来说，这个消息头在发送方向上也是同一个键：把它
写到一条外发消息上，这条消息就有序了，而点名步骤的调用优先于它。它绝不会作为属性传输。

有序投递需要在订阅上开启消息排序。要让一个键在一个区域内跨多个发布者保持有序，靠的是区域性的
`endpoint`。在有序键上返回错误的发布会在客户端把该键暂停；本 crate 会恢复该键并返回错误，因此
一次失败不会挡住这个键上之后的每一次发布。

其余每个消息头在两个方向上都是消息属性，外面不套信封，所以非 Rust 的对端看到的是一条普通的
Pub/Sub 消息。

## 发布 { #publishing }

`PubSubPublish` 是构造发布者 `PubSubPublisher` 的策略，运行时在启动时在已连接的 Broker 上实例化
它。它也是该 Broker 的默认策略，所以挂载时没有调用 `.out(Reply, ..)` 的回复型处理器就经由它发布。

装处理器主体的文件只导入框架的 prelude，并用一个能力约束它的槽位：`Out(out): Out<impl Publisher>`。
这样的文件不点名任何 Broker。例外是逐条点名[排序键](#ordering-keys)的主体。

路由文件导入 `ruststream_gcp_pubsub::prelude::*`，策略在那里以挂载点名字 `Publish` 出现：回复型
处理器返回的值用 `.out(Reply, Publish::default())`，`Out` 槽位用
`.out(Marker, Publish::default())`，整个槽位都有序时在策略上用 `.ordering_key(..)`。
`PubSubPublish` 留在 crate 根部，供同时对两个 Broker 说话、必须讲清指的是哪一个的文件使用。

一次发布寻址一个主题，用 id 或完整资源名。某个主题的客户端发布者在首次使用时创建，并缓存在
Broker 上，所以 `shutdown` 会冲刷掉它缓冲的每一批。

`ordering_key` 是本 crate 对框架发布链唯一的添加。一条 Pub/Sub 消息就是载荷、它的属性和一个
排序键，所以这条链把三者都覆盖了：值、它的消息头和键。

手工构造并交给 `Publisher::publish` 的消息，按构造出来的样子发出去，你要自己掌控整组消息头时
走这条路。

## 模拟器 { #the-emulator }

`emulator(host)` 把 Broker 指向本地的 Pub/Sub 模拟器：一次调用同时给出明文地址和匿名凭据。客户端
不读 `PUBSUB_EMULATOR_HOST`，所以主机由你自己指定。

```bash
just brokers-up    # docker compose up：在 8085 上 gcloud beta emulators pubsub start
cargo run --example pubsub_service
just brokers-down
```

模拟器起来时是空的，`create_with_topic` 就是为此而设：订阅和它的主题在订阅时创建。两个服务抢同样
的名字，最后都能连上，因为创建是一次读取、再一次创建、再一次读取。

实时测试套件用同样的方式跑，由 `PUBSUB_TEST_HOST` 开关控制：

```bash
just test-brokers  # 启动模拟器，跑集成套件和 conformance 套件
```

## 测试 { #testing }

`testing` feature 提供 `PubSubTestBroker`：一个复刻本 crate 路由行为的进程内传输。把服务挂到它
上面，用框架的 `TestApp` 测试套件驱动它。测试文件在 prelude 的 glob 旁边按路径点名这两个类型：
`use ruststream_gcp_pubsub::testing::PubSubTestBroker;` 和
`use ruststream::testing::TestApp;`。

`TestApp::start(app)` 连接应用并挂载它的处理器。
`tb.broker::<PubSubTestBroker>().message(&order).to("orders-workers").publish()` 把一份订单投递到
处理器的订阅，`tb.settle()` 等待这次反应结束。

随后 `tb.broker::<PubSubTestBroker>().subscriber("orders-workers").assert_called_once()` 断言处理
器跑过了，
`tb.broker::<PubSubTestBroker>().published::<Receipt>("receipts").assert_called_once().with(&expected)`
断言它向主题发布了什么。参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

路由文件按出厂的样子挂到这个进程内传输上。`#[subscriber(GooglePubSub::new("orders-workers"))]`
打开一条进程内订阅，和它打开流式拉取一样顺手，`Publish` 在它上面构造发布者，和在 Broker 上一样。
两边都没有要临时换上的测试专用写法。

`tb.out::<Marker>().with_options(&PubSubPublishOptions { ordering_key: Some("order-7".to_owned()) })`
读回某次经槽位的发布所要求的键，`assert_options_default()` 则说明这次发布没有点名键，用的是挂载点
的键。这个键也会进到已发布的消息里，就在投递报告它所用的那个 `partition-key` 消息头下，所以两种
断言都可用。

进程内传输只按一个地址路由，也就是订阅名，因为它不持有主题。因此测试注入的是订阅，而不是主题。
`batch_wait` 生效，因为攒批次本来就在客户端。`create_with_topic` 无物可建，`max_outstanding` 和
`ack_extension` 点的是这里并不存在的机制，所以这三项一律忽略。

活过 `shutdown` 的发布者在这里报告 `NotConnected`，和它对着 Pub/Sub 时一样，所以测试不会在一次
产品本身会拒绝的发布上变绿。

服务器自己决定的那些事（截止时间续期、重新投递的时机、有序投递和死信策略）对着
[模拟器](#the-emulator)检查，集成套件和框架的 conformance lifecycle 都在那里跑。
