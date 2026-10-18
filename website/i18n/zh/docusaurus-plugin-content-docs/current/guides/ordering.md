---
title: 排序键
sidebar_position: 6
description: "在 Pub/Sub 上让同一个键的消息保持顺序：有序订阅、服务内按键划分的工作分区，以及所发布消息上的键。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 排序键

本指南让同一个键的消息保持顺序：进入服务时、在服务自身的并发中，以及离开服务时。

Pub/Sub 默认不以任何特定顺序投递消息。
带排序键发布、发往启用了消息排序的订阅的消息，会在同一个键的所有更早的消息之后到达。
不同键的消息仍以任意顺序到达，正因如此它们才能并行处理。
这一保证对发布到同一区域的消息成立。

示例是一个账簿服务：每笔分录改变一个账户的余额，同一账户的分录必须按顺序应用。
账户服务以账户 id 作为排序键发布每笔分录。

## 有序投递，按工作分区处理 {/* #ordered-delivery-handled-in-lanes */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#post"
```

</TabItem>
</Tabs>

[`create_with_topic`](../components/subscriptions/create-with-topic.md) 创建启用了消息排序的 `ledger-posting`。
Pub/Sub 在创建订阅时固定这项设置，所以归运维所有的订阅只有在运维开启时才具备它。

`workers(8, by_key)` 用八个工作分区同时处理最多八笔分录。
同一账户的每笔分录都进入同一个工作分区，一个工作分区依次处理它的分录。
因此不同账户并行记账，同一账户的分录保持顺序。
投递在 [`partition-key`](../components/publishing/ordering/partition-key.md) 消息头中报告它的排序键，工作分区读取的就是这个键。
在手写形式中，工作分区是挂载链上的 `workers_by_key` 步骤。

当一个键的某次投递被重新投递时，Pub/Sub 会把这个键之后的消息也再次投递，所以有序订阅的处理器是幂等的。

## 服务所发布消息上的键 {/* #the-key-on-what-the-service-publishes */}

`post` 以账户的键把记好的分录发布到 `postings`，键在调用时用 [`ordering_key`](../components/publishing/ordering/ordering-key.md) 步骤指定。
这个步骤随预导入模块（prelude）提供，它要求 `Out` 参数指定本 crate 的设置类型：`Publisher<Options = PubSubPublishOptions>`。
不带键的发布没有顺序。

应答没有可以指定键的调用。
[发布变换](../components/publishing/ordering/transform.md)按每次投递设置它：

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#notify"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#notify"
```

</TabItem>
</Tabs>

`UnderTheEntrysKey` 读取分录到达时带的键，把它交给对账单，因此同一账户的对账单也保持顺序。
对挂载点的每条消息都相同的键，改为放在策略上：[`Publish::default().ordering_key(..)`](../components/publishing/ordering/policy.md)。
几处都指定了键时，调用优先于 `partition-key` 消息头，消息头优先于策略。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

带键的发布失败时，客户端暂停这个键；本 crate 恢复它，因此该键的下一次发布照常发出，而这次失败作为错误返回给处理器。
使用模拟器时，在服务启动之前创建主题 `postings` 和 `statements`，做法同[发布与应答](publishing.md#running-it)。

## 测试 {/* #testing */}

测试在 `partition-key` 消息头中带上账户的键发布一笔分录，账户服务的键正是这样到达投递的。
它们用 `tb.out::<DefaultSlot>().with_options(..)` 检查记账消息上的键，用 `published(..).with_options(..)` 检查对账单上的键。
进程内 Pub/Sub 不模拟有序投递；[排序组件页](../components/publishing/ordering/index.md)中有一个在模拟器上运行的有序投递测试。
