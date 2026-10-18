---
title: 租约
sidebar_position: 1
description: "流式拉取为每条消息加上租约：处理器工作期间，客户端延长它的确认截止时间，流控限制客户端持有的消息数量。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "流式拉取：消息带着租约到达，应用持有消息期间客户端不断延长租约，ack 按批发回",
  "text": "以文字阅读本图",
  "subscription": ["Pub/Sub 订阅", "每条消息一个确认截止时间"],
  "client": "google-cloud-pubsub",
  "pull": ["流式拉取", "上限 max_outstanding"],
  "lease": ["租约集合", "每 3 秒检查一次"],
  "flush": ["ack 和 nack", "按批发送，", "每 100 毫秒", "发一次"],
  "app": "应用",
  "handler": ["处理器", "持有这条消息"],
  "settle": ["结算它：", "ack 或 nack"],
  "expired": ["超过 max_lease 后客户端不再延长：", "截止时间届满，消息回到订阅"],
  "messages": "消息",
  "extends": "延长",
  "settled": "已结算",
  "answers": "ack、nack"
};

# 租约

流式拉取为每条消息加上租约：处理器工作期间，客户端延长它的确认截止时间，流控限制客户端持有的消息数量。

<Diagram name="leases" words={words} />

流式拉取是每个订阅一条长期存在的 gRPC 流。消息一到，服务就沿这条流发下来，每条消息都带有一个确认截止时间。截止时间之前既没有确认、也没有延长的消息会再次投递，可能投递给另一个订阅者。

客户端库负责推迟这个截止时间。应用仍持有的每条消息，它都发送 `modifyAckDeadline`，每次把截止时间最多向后推 [`ack_extension`](api:ruststream_gcp_pubsub::GooglePubSub::ack_extension)（默认 60 秒，限定在 10 到 600 秒之间）。这样的延长最多持续 [`max_lease`](api:ruststream_gcp_pubsub::GooglePubSub::max_lease)，默认一小时。进程终止后不再发送延长请求，它的消息在截止时间过后回到订阅。

流控作用在流上。流上未结算的消息达到 [`max_outstanding`](api:ruststream_gcp_pubsub::GooglePubSub::max_outstanding)（默认 1000 条）或 100 MiB 时，服务停止发送；数量降到上限以下后，服务恢复发送。因此，慢的处理器会拖慢它自己的流，它还没收到的消息留在订阅中等待。

ack 把消息从订阅中移除。nack 是截止时间为零的 `modifyAckDeadline`：消息离开租约集合，服务再次投递它，[重新投递](redelivery.md)一页用图说明了这一点。

这些数字来自 `google-cloud-pubsub` 1.3 客户端（[`Subscribe`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/builder/subscriber/struct.Subscribe.html)）和 Google 的文档（[租约管理](https://docs.cloud.google.com/pubsub/docs/lease-management)）。
