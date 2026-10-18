---
title: 流控与租约
sidebar_position: 5
description: "限制服务持有的 Pub/Sub 消息数量、每次租约延长的时长，以及慢消息保持租约的时间。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 流控与租约

本指南限制一个订阅同时持有的消息数量，并在服务处理或等待一条慢消息时让它保持租约。

Pub/Sub 为它在流式拉取上投递的每条消息加上租约。
租约有效期间，订阅不把这条消息投递给任何其他消费者。
客户端在后台延长租约，直到消息结算，但有上限。
租约到期后，消息会交给另一个消费者，即使第一个消费者仍在处理它。

示例是一个报表服务：渲染报表需要一段时间，有时它的数据还没有到。

## 订阅 {/* #the-subscription */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/flow_control.rs#render"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/flow_control.rs#render"
```

</TabItem>
</Tabs>

三项设置都是 [`GooglePubSub`](../components/subscriptions/google-pubsub.md) 描述符的步骤。

[`max_outstanding(10)`](../components/subscriptions/max-outstanding.md) 限制服务同时持有的未结算消息数量。
达到上限时，Pub/Sub 停止发送；消息结算后，它恢复发送。
被 `retry_after` 扣留的消息在回来之前计入上限。
客户端的默认值是 1000；较小的上限让慢工作不会堆积在一个副本中，而其他副本却闲着。

[`ack_extension`](../components/subscriptions/ack-extension.md) 是每次延长租约的时长，这里是一分钟。
客户端把它限定在 10 秒到 10 分钟之间。
延长得越长，对 Pub/Sub 的调用越少；延长得越短，副本宕机后消息越早交给另一个消费者。

[`max_lease`](../components/subscriptions/max-lease.md) 是客户端持续延长一个租约的时间，这里是 30 分钟，默认一小时。
它也是 `retry_after` 可以要求的最长延迟，因为延迟是在租约下度过的。

## 在租约下等待 {/* #waiting-under-the-lease */}

数据迟到的报表，`render` 用 [`retry_after`](../components/settlement/retry-after.md) 要求 20 秒后再投递一次。
服务在延迟期间保留这条消息，客户端延长它的租约，因此其间没有其他消费者拿到它。
延迟结束时，服务 nack 这条消息，Pub/Sub 再次投递它。
长于 `max_lease` 的延迟会超出租约，所以服务拒绝它，消息立即回来。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/flow_control.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/flow_control.rs#app"
```

</TabItem>
</Tabs>

应用由 `service(broker)` 构建，所以测试可以在进程内和模拟器上运行同一个服务。

## 测试 {/* #testing */}

测试发布一份数据迟到的报表，并检查它在 20 秒后回来一次。
在进程内，延迟在暂停的时钟上流逝。
进程内 Pub/Sub 没有租约，也没有流控，所以同一个测试也在模拟器上运行，那里的 20 秒是真实的。
在那里，等待时间是订阅 10 秒确认截止时间的两倍，而报表只回来一次：租约留住了它。

```bash
docker compose up -d
PUBSUB_TEST_HOST=localhost:8085 cargo test -- --test-threads=1
```

在线测试从 `PUBSUB_TEST_HOST` 读取模拟器的地址，未设置时跳过。
同一个测试主体如何在两种模式下运行，见[测试 Pub/Sub 服务](testing.md)。
