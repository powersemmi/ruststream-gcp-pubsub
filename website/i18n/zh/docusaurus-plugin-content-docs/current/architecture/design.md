---
title: 设计
sidebar_position: 2
description: "ruststream-gcp-pubsub 的位置：在 RustStream 运行时与 google-cloud-pubsub 客户端之间，每个订阅一个流式拉取，每个主题一个发布者。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、google-cloud-pubsub 客户端和 Pub/Sub",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、worker、结算",
  "crate": "ruststream-gcp-pubsub",
  "subscriber": ["PubSubSubscriber", "每个订阅", "一个泵任务"],
  "publishers": ["PubSubPublisher", "每个主题一个", "客户端发布者"],
  "message": ["PubSubMessage", "持有租约"],
  "broker": ["PubSubBroker", "配置"],
  "descriptor": ["GooglePubSub", "订阅"],
  "inProcess": ["进程内传输", "testing 特性下"],
  "client": "google-cloud-pubsub",
  "pull": ["Subscriber", "流式拉取"],
  "clients": ["Publisher、", "TopicAdmin 等"],
  "pubsub": "Pub/Sub 或模拟器",
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-gcp-pubsub` 所处的位置：在 RustStream 运行时与 `google-cloud-pubsub` 客户端之间，每个订阅一个流式拉取，每个主题一个发布者。

<Diagram name="design" words={words} />

本 crate 在 Google 的 [`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/) 客户端之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。Pub/Sub 特有的一切都在本 crate 里：[订阅描述符](api:ruststream_gcp_pubsub::GooglePubSub)、每种处理器结果对租约做什么，以及发布如何设置排序键。

connect 构建四个客户端，并在连接的整个生命周期内共用它们：`Subscriber`、`BasePublisher`、`TopicAdmin` 和 `SubscriptionAdmin`。两个管理客户端创建描述符声明的拓扑，并写入注册声明的死信政策。[`emulator`](api:ruststream_gcp_pubsub::PubSubBroker::emulator) 让每个客户端以匿名凭据连接本地模拟器。否则，客户端使用给定的 [`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint) 和 [`credentials`](api:ruststream_gcp_pubsub::PubSubBroker::credentials)，未给定时使用应用默认凭据（Application Default Credentials）。

每个订阅运行一个泵任务。它读取客户端的流，把每次投递转发进一个容量为 16 的通道，运行时轮询这个通道。批由这个通道上的投递组装而成。客户端的租约保存在每个 [`PubSubMessage`](api:ruststream_gcp_pubsub::PubSubMessage) 里，因此结算直接交给客户端。

同一连接的[发布者](api:ruststream_gcp_pubsub::PubSubPublisher)为每个主题共用一个客户端发布者，在该主题第一次发布时创建。客户端把它们发送的消息合成批。

在 `testing` 特性下，[`TestApp`](api:ruststream::testing::TestApp) 把同一个 Broker 连接到进程内传输，而不是这些客户端。它模拟主题和订阅、相互竞争的订阅者、确认和重新投递、尝试计数和死信转移。不启用该特性的构建不包含其中任何部分。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。
