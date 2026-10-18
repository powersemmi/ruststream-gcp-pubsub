---
title: 路径
sidebar_position: 4
description: "一条 Pub/Sub 消息在 RustStream 服务中的路径：流式拉取、投递、处理器、应答和 ack。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的 Pub/Sub 消息的路径：从流式拉取到它的 ack",
  "text": "以文字阅读本图",
  "pull": ["流式拉取", "客户端为消息加上租约"],
  "pump": ["泵任务", "转发进容量为 16 的通道"],
  "message": ["PubSubMessage", "属性成为消息头，不做复制"],
  "dispatch": ["分发", "一个循环，或多个 worker"],
  "decode": ["解码", "直接读取消息的数据"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进缓冲区，再整个交出"],
  "publisher": ["PubSubPublisher", "等待消息 id"],
  "settle": ["结算", "ack、nack，或扣留的重试"],
  "ack": ["ack", "随客户端的下一批发出"]
};

# 路径

一条 Pub/Sub 消息在 RustStream 服务中的路径：流式拉取、投递、处理器、应答和 ack。

<Diagram name="path" words={words} />

客户端从流中取出消息，开始它的租约。泵任务把它包装成 [`PubSubMessage`](api:ruststream_gcp_pubsub::PubSubMessage)，不做复制：数据仍是客户端的缓冲区，属性移入消息头。排序键成为 `partition-key` 消息头；订阅有死信政策时，尝试次数成为 `pubsub-delivery-attempt` 消息头。

处理器的应答编码进一个缓冲区，这个缓冲区原样成为外发消息的数据，因为客户端会保留它发布的字节。消息头成为属性；不是 UTF-8 的消息头会让发布失败，因为属性是文本。为这次发布指定的键成为消息的排序键，[排序](pubsub/ordering.md)一页用图说明了这一点。

发布者在服务为消息分配 id 之后才返回。应答发布失败时，这次投递以重试的方式结算，因此消息会回来。

投递在应答之后结算。ack 随客户端的下一批确认发出，大约在 100 毫秒之内。每种结果变成什么，见[结算](settlement.md)。
