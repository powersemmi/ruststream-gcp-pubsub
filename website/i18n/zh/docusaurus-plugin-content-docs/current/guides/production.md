---
title: 在生产环境中运行
sidebar_position: 8
description: "在 Google Cloud 上运行 Pub/Sub 服务：凭据、区域端点、归运维所有的拓扑、IAM 角色，以及在单一区域内的精确一次。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

本指南把服务从模拟器带到 Google Cloud：它如何登录、连接哪个端点、拓扑归谁所有，以及它需要哪些角色。

示例是一个从环境变量读取配置的支付服务。

## 凭据与端点 {/* #credentials-and-the-endpoint */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

不调用 [`emulator`](../components/broker/emulator.md) 时，Broker 连接 Google Cloud。
它用应用默认凭据（Application Default Credentials）登录，所以代码中不出现凭据。
在 GKE、Cloud Run 和 Compute Engine 上，凭据来自元数据服务器：服务以其工作负载的服务账号运行。
在开发机上，`gcloud auth application-default login` 写入凭据；在其他环境中，`GOOGLE_APPLICATION_CREDENTIALS` 指定一个密钥文件。
必须以特定账号登录的服务，用 `google-cloud-auth` 构建一个凭据值，并把它传给 [`credentials`](../components/broker/credentials.md)。
客户端在第一次调用 Pub/Sub 时才请求令牌，所以凭据错误会表现为那次调用的错误：启动时的拓扑调用、订阅的流，或者一次发布。

全局端点把每个请求路由到附近的区域。
[`endpoint`](../components/broker/endpoint.md) 把服务固定在一个区域，这里取自 `PUBSUB_ENDPOINT`。
有两项保证需要这样做：同一[排序键](ordering.md)的消息只有发布到同一区域时才按顺序到达；精确一次订阅只对其所在区域内的订阅者保证只投递一次。
端点作用于 Broker 的每个客户端，包括启动时的拓扑调用。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

在生产环境中，主题和订阅通常属于基础设施，由 Terraform 或 `gcloud` 连同保留期、过滤器和重试政策一起创建。
这时服务只指定它的订阅 `#[subscriber("payments-workers")]`，不创建任何东西。
死信声明仍然生效：启动时，服务把它作为死信政策写到订阅上。
如果也想把这个政策保留在基础设施中，运维设置同样的上限和主题，启动时的更新就不会改变任何东西。
[`create_with_topic`](../components/subscriptions/create-with-topic.md) 适合拥有自己拓扑的服务，以及模拟器。

Pub/Sub 拒绝第一个拓扑调用时，启动就此停止，并返回指明资源的 [`PubSubError::Admin`](api:ruststream_gcp_pubsub::PubSubError::Admin) 错误。
订阅不存在时，它的流返回 [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive) 错误。

## 角色 {/* #roles */}

服务的服务账号需要以下权限：

| 服务的操作 | 权限 | 预定义角色 |
|---|---|---|
| 读取订阅 | 该订阅上的 `pubsub.subscriptions.consume` | `roles/pubsub.subscriber` |
| 向主题发布 | 该主题上的 `pubsub.topics.publish` | `roles/pubsub.publisher` |
| 在订阅上声明死信政策 | 该订阅上的 `pubsub.subscriptions.update` | `roles/pubsub.editor` |
| 使用 `create_with_topic` | 项目中的 `pubsub.topics.get`、`pubsub.topics.create`、`pubsub.topics.attachSubscription`、`pubsub.subscriptions.get` 和 `pubsub.subscriptions.create` | `roles/pubsub.editor` |

死信政策还需要项目的 Pub/Sub 服务代理 `service-{project-number}@gcp-sa-pubsub.iam.gserviceaccount.com`。
它向死信主题发布，所以需要该主题上的 `roles/pubsub.publisher`；它确认被转移的消息，所以需要源订阅上的 `roles/pubsub.subscriber`。

## 精确一次投递 {/* #exactly-once-delivery */}

精确一次是订阅的设置，由运维在创建订阅时设定。
本 crate 不需要任何设置：在这样的订阅上，每次确认都要等 Pub/Sub 确认，被 Pub/Sub 拒绝的确认会返回一个指明原因的错误。
服务通过订阅所在区域的区域端点连接，因为这一保证只在该区域内成立。
处理器有哪些变化，见[确认与重试](retries.md#exactly-once-subscriptions)。

## 测试 {/* #testing */}

测试用自己的 `Config` 在进程内启动同一个服务，那里不建立任何连接，也不读取任何凭据。
`app` 读取环境变量，所以没有 `GOOGLE_CLOUD_PROJECT` 时，`main` 在建立任何连接之前就立即返回错误。
