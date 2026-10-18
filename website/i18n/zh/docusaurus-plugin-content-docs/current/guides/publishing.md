---
title: 发布与应答
sidebar_position: 3
description: "从服务向 Pub/Sub 主题发布：类型指定主题的应答、处理器中的 Out 槽位，以及启动时发布的消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 发布与应答

本指南用服务发布消息的三种方式向 Pub/Sub 主题发布：应答一条消息、在处理器内部发布，以及在服务就绪后发布一条消息。

每次发布都经过一个发布策略。
[`Publish`](../components/publishing/index.md) 是预导入模块（prelude）中 [`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish) 的名字。它向主题发布，并等待服务接受这条消息。
主题不存在时发布返回错误：Pub/Sub 不为它没有的主题保留消息。

示例是一个订单服务：它为每个订单应答一张收据，为订单开账单，并通告自己已经启动。

## 应答 {/* #a-reply */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

`reply` 子句让返回值成为一条要发布的消息。
它发往哪里，是它的类型的属性：`#[outgoing(name = "receipts")]` 把每个 `Receipt` 发往主题 `receipts`。
收据在处理器返回之后、订单确认之前发布。因此收据发布失败时，订单保持未确认，并会回来。

## 在处理器中发布 {/* #a-publish-from-a-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

`Out` 参数是处理器自己调用的发布者。
`bill` 在调用时用 `.to("invoices")` 指定主题，并决定发布失败对订单意味着什么。
这里它用 `retry` 要求再次投递订单，因此账单不会丢失。
处理器在结算之前可以发布任意数量的消息，发往任意主题。

## 挂载点 {/* #the-mount-site */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish::default())` 为应答指定策略，`.out(DefaultSlot, Publish::default())` 为 `Out` 参数指定它自己的策略。
策略保存其位置上每条消息的设置：[`Publish::default().ordering_key(..)`](../components/publishing/ordering/policy.md) 就是一例。
Broker 为每个主题创建一个客户端发布者，在第一次向该主题发布时创建，此后向该主题的每次发布都复用它。

`after_startup` 在每个订阅都打开之后运行一次。
它收到所指定策略的一个可用发布者，这里用来在 `services` 上通告服务。
钩子返回错误会中止启动，所以无法通告自己的服务不会运行。

## 运行 {/* #running-it */}

服务发布到的主题属于读取它们的一方，所以在服务启动之前就已存在。
使用模拟器时，通过它的 REST API 创建这些主题：

```bash
for topic in receipts invoices services; do
  curl -X PUT localhost:8085/v1/projects/my-project/topics/$topic
done
cargo run -- run
```

没有 `services` 时，通告失败，服务不会启动。

## 测试 {/* #testing */}

示例文件中的测试发布一个订单，并用 `published` 断言到达 `receipts` 和 `invoices` 的内容。
第二个测试启动应用，并在 `services` 上找到通告。
在进程内，每次发布都会被记录，不论是否有订阅读取它的主题。
测试套件的用法见[测试 Pub/Sub 服务](testing.md)。
