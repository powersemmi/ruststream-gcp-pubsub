---
title: 测试 Pub/Sub 服务
sidebar_position: 9
description: "用 TestApp 测试 Pub/Sub 服务：在进程内运行生产应用，覆盖应答、丢弃、延迟和死信，并在模拟器上运行同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 Pub/Sub 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 Pub/Sub](../components/testing/index.md) 或者模拟器。

被测服务负责退款。
退款到达 `refunds`，处理器在 `refunds-done` 上以一条 `Refunded` 消息应答。
没有金额的退款被丢弃。
超过审批限额的退款用 [`retry_after`](../components/settlement/retry-after.md) 等待一分钟，以待审批。
经过五次投递后，退款进入[死信主题](../components/settlement/dead-letters.md) `refunds-review`。
应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用连接模拟器的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-gcp-pubsub` 的 `testing` feature。

## 得到应答的退款 {/* #a-refund-answered */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#answered"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#answered"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`PubSubBroker`](../components/broker/index.md) 连接到进程内 Pub/Sub。
测试不建立任何连接，也不读取任何凭据。
测试按生产类型访问 Broker：`tb.broker::<PubSubBroker>()`。
`Refund` 指定了自己的主题，所以 `message(..).publish()` 不需要目的地。
`publish` 在处理器及其应答都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `refunds-done`。
测试主体把已启动的应用作为参数接收，所以下面的在线测试也运行它。

## 被丢弃的退款 {/* #a-refund-dropped */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何应答。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟向前走。
一分钟的 `APPROVAL_WAIT` 不花任何时间。
每次等待都以同一条消息的重新投递结束，`assert_called` 统计这些投递。
第五次投递同样要等待，等待结束时，Pub/Sub 把退款按到达时的样子转到 `refunds-review`。
`published` 读取到达某个主题的内容，不论是服务还是 Pub/Sub 放到那里的。

## 进程内 Pub/Sub 模拟了什么 {/* #what-the-in-process-pubsub-models */}

进程内 Pub/Sub 读取生产 Broker 的项目，以及服务挂载的每个描述符。
Pub/Sub 拒绝的发布、订阅或声明，会以同样的方式让测试失败。
它模拟的是 Pub/Sub，而不是一个队列：

- 主题和订阅相互独立，主题的每个订阅都收到它创建之后发布的每条消息。
- 同一订阅的消费者分担它的消息。
- `retry` 再次投递消息，`retry_after` 先在测试的时钟上等待，`drop` 确认消息。
- 死信政策统计投递次数，并转移用尽次数的消息。

服务预期找到的拓扑被视为已经存在。
服务只指定名称的订阅读取与它同名的主题，服务发布到的主题接受消息。
租约、流控、按键的有序投递和精确一次投递需要模拟器或 Pub/Sub 本身。

## 在模拟器上运行同一个测试 {/* #the-same-test-against-the-emulator */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到模拟器。
这时一次发布最多等待十秒，直到它到达的每个订阅都处理完它；`start_live_within` 设置另一个期限。
测试用 `live::host` 从 `PUBSUB_TEST_HOST` 读取模拟器的地址，未设置时跳过；`live::host` 是示例自己的辅助函数。
模拟器在多次运行之间保留它的订阅，所以 `live::fresh` 先删除 `refunds-workers`，本次运行只读取它自己发布的消息。
`create_topic` 像运维那样创建 `refunds-done`，因为发往不存在主题的应答会失败。
断言与进程内的完全相同，未作改动。

```bash
cargo test
PUBSUB_TEST_HOST=localhost:8085 cargo test -- --test-threads=1
```
