---
title: 结算
sidebar_position: 5
description: "ack、retry、retry_after 和 drop 在 Pub/Sub 上变成什么；Pub/Sub 对每条消息确认、拒绝或继续租用。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "处理器的每种结果，以及本 crate 为它在 Pub/Sub 上发出的调用",
  "text": "以文字阅读本图",
  "handler": "处理器",
  "onPubSub": "在 Pub/Sub 上",
  "ack": ["ack()"],
  "acked": ["已确认：", "从订阅中移除"],
  "retry": ["retry()"],
  "nacked": ["nack：按重试政策", "等待后再次投递，", "或转入死信"],
  "retryAfter": ["retry_after(d)"],
  "held": ["扣留 d，其间客户端", "继续延长租约，", "然后 nack；若 d 超过", "max_lease 则拒绝"],
  "drop": ["drop()"],
  "dropped": ["已确认：", "不再投递"],
  "dropAtCap": ["最后一次尝试", "上的 drop()"],
  "deadLettered": ["nack，于是服务", "把它转到", "死信主题"],
  "exactlyOnce": ["在精确一次订阅上，ack 和 nack", "要等服务确认"]
};

# 结算

`ack`、`retry`、[`retry_after`](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/retry-after/) 和 `drop` 在 Pub/Sub 上变成什么；Pub/Sub 对每条消息确认、拒绝或继续租用。

<Diagram name="settlement" words={words} />

Pub/Sub 除了 ack 之外，没有“丢弃且不重新投递”的操作，因此 `drop()` 执行 ack。例外是订阅的死信政策允许的最后一次投递。在那里，nack 才会把消息转到死信主题，因此 `drop()` 执行 nack。本 crate 从投递中读取尝试次数。上限来自注册的 [`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts)；如果运维人员在代码之外设置了政策，则在启动时从订阅本身读取一次。读取需要 `pubsub.subscriptions.get` 权限；没有该权限时，服务会记录一条警告，`drop()` 执行 ack。尝试次数是近似值，因此计数被重置时，`drop()` 同样执行 ack。

解码失败默认丢弃消息（[`FailurePolicy`](api:ruststream::runtime::FailurePolicy)），因此在 Pub/Sub 上这条消息得到确认。

订阅自己统计尝试次数，并自己转移用尽次数的消息。声明了 `max_attempts` 时，`retry()` 与其他 nack 无异，由服务执行上限。在这个 Broker 上，运行时不发布重试副本。

`retry_after(d)` 把投递留在进程中。Broker 建立连接时所在的运行时上，有一个任务等待 `d`，然后 nack。在此期间，客户端继续延长租约，消息计入 [`max_outstanding`](api:ruststream_gcp_pubsub::GooglePubSub::max_outstanding)。`d` 长于 [`max_lease`](api:ruststream_gcp_pubsub::GooglePubSub::max_lease) 时，调用直接返回 `PubSubError::DelayBeyondLease`，因为租约会先结束。如果进程在等待期间终止，租约随之结束，服务再次投递这条消息：延迟丢失了，消息没有丢失。

在精确一次订阅上，本 crate 使用客户端带确认的 ack 和 nack。返回的 ack 已由服务接受，服务拒绝的 ack 会作为失败的确认写入日志。在其他订阅上，ack 是尽力而为的，消息仍可能回来。
