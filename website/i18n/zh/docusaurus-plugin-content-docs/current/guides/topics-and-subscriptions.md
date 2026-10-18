---
title: 主题与订阅
sidebar_position: 2
description: "通过订阅读取 Pub/Sub 主题：每个消费者一个订阅实现扇出，副本共用订阅，订阅由服务或运维创建。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 主题与订阅

本指南让服务的几个部分读取同一个主题，所用的订阅有的由服务创建，有的归运维所有。

发布者向主题发送消息。
订阅是一种独立的资源，挂在一个主题上。它保留创建之后发布到该主题的每条消息。
主题的每个订阅都收到每条消息。
同一订阅的消费者分担它的消息。
服务读取的总是订阅。

示例是一个订单服务：计费、发货、审计和报表各自读取主题 `orders`。

## 每个消费者一个订阅 {/* #one-subscription-per-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
</Tabs>

`bill` 和 `ship` 在 `orders` 上各有自己的订阅，因此各自收到每个订单。
主题的新消费者就是一个新订阅，发布者一方不需要任何改动。
订阅只收到它存在之后发布的消息，所以后来加入的消费者看不到此前的订单。

[`GooglePubSub`](../components/subscriptions/google-pubsub.md) 用 Broker 所在项目中的短 id 指定订阅，也可以用完整的资源名称 `projects/{project}/subscriptions/{id}`。
[`create_with_topic`](../components/subscriptions/create-with-topic.md) 让服务拥有自己的拓扑：启动时，它创建缺失的主题和订阅，已经存在的则保持不动。
它创建的订阅启用消息排序，[排序键](ordering.md)一页会用到这一点。

## 归运维所有的订阅 {/* #subscriptions-operations-own */}

在生产环境中，拓扑通常属于基础设施，在服务运行之前由 Terraform 或 `gcloud` 创建。
这时服务只需指定它读取什么。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
</Tabs>

`#[subscriber("orders-audit")]` 按原样读取[订阅](../components/subscriptions/index.md) `orders-audit`。
启动时不创建任何东西。订阅不存在时，它的流返回 [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive) 错误。
`#[subscriber(GooglePubSub)]` 固定订阅的种类，名称留空。
挂载点用 [`.name(..)`](../components/subscriptions/name.md) 指定订阅，由部署决定名称，这里取自 `REPORTS_SUBSCRIPTION`。
在手写形式中，描述符在挂载处理器的地方构建，所以直接接收名称。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
</Tabs>

使用模拟器时，像运维那样，在服务启动之前创建这两个订阅：

```bash
for subscription in orders-audit orders-reports; do
  curl -X PUT localhost:8085/v1/projects/my-project/subscriptions/$subscription \
    -H 'content-type: application/json' \
    -d '{"topic": "projects/my-project/topics/orders"}'
done
```

服务至少启动过一次之后，主题 `orders` 就存在了，因为计费和发货会创建它。

## 服务的多个副本 {/* #several-replicas-of-the-service */}

服务的每个副本都在同一组订阅上打开流式拉取。
Pub/Sub 把订阅的每条消息交给其中一个副本，因此副本分担工作。
横向扩展就是启动更多副本，不需要改任何设置。
一个副本用 `retry` 交还的消息，可能回到另一个副本。

在一个副本内部，一个订阅一次处理一条消息。
`#[subscriber]` 属性上的 `workers(n)` 让它同时处理最多 `n` 条；按键分配的形式见[排序键](ordering.md)。

## 测试 {/* #testing */}

示例文件中的测试向 `orders` 发布一个订单，并检查计费和发货都处理了它。
在进程内，服务只指定名称的订阅读取与它同名的主题，所以 `orders-audit` 和 `orders-reports` 的测试向这两个主题发布。
进程内 Pub/Sub 模拟了什么，见[测试 Pub/Sub 服务](testing.md)。
