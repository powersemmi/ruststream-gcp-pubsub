---
title: 主题
sidebar_position: 0
description: "Pub/Sub 主题把每条消息交给挂在它上面的每个订阅，同一订阅的订阅者分担这个订阅的消息。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "发布者向一个主题发布；两个订阅各自收到每条消息，同一订阅的两个订阅者分担消息",
  "text": "以文字阅读本图",
  "publisher": "发布者",
  "topic": "主题 orders",
  "billing": "订阅 billing",
  "billing1": ["订阅者 1：", "消息 1 和 3"],
  "billing2": ["订阅者 2：", "消息 2"],
  "audit": "订阅 audit",
  "audit1": ["唯一的订阅者：", "消息 1、2、3"],
  "retained": ["订阅保留消息，直到消息得到确认", "或保留期届满，不论有没有订阅者"],
  "publishes": "发布",
  "copy": "每条消息"
};

# 主题

Pub/Sub 主题把每条消息交给挂在它上面的每个订阅，同一订阅的订阅者分担这个订阅的消息。

<Diagram name="topics" words={words} />

发布者只指定一个主题，不指定别的。订阅是一种独立的资源，恰好挂在一个主题上。一个主题可以有多个订阅。

订阅收到的是它创建之后发布到其主题的消息。每个订阅都得到每条消息的一份副本，因此 billing 和 audit 绝不会拿走对方的消息。

同一订阅的订阅者分担它的消息：每个订阅者得到一部分，任意两个订阅者得到的部分互不重叠。给订阅增加订阅者，会分散它的负载。增加订阅，则多了一个读取全部消息的读者。

在 `ruststream-gcp-pubsub` 中，处理器通过 [`GooglePubSub`](api:ruststream_gcp_pubsub::GooglePubSub) 或直接用名称指定它读取的订阅，发布者指定它写入的主题。这一模型就是 Google 自己的模型（[订阅概览](https://docs.cloud.google.com/pubsub/docs/subscription-overview)、[Pub/Sub 基础知识](https://docs.cloud.google.com/pubsub/docs/pubsub-basics)）。订阅者在处理消息期间如何持有它，见[租约](leases.md)。
