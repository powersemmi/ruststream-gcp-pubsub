---
title: 排序
sidebar_position: 2
description: "共享同一排序键、在同一区域发布的消息，按发布顺序到达启用了消息排序的订阅。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "同一发布者的两个排序键：每个键一次发布一批，并按各自的顺序投递",
  "text": "以文字阅读本图",
  "publisher": ["发布者", "每个键一批在途"],
  "keyA": ["键 order-1：", "#1、#2、#3"],
  "keyB": ["键 order-2：", "#1、#2"],
  "topic": ["主题", "在同一区域发布"],
  "subscription": "启用消息排序的订阅",
  "deliveredA": ["order-1：", "先 #1，再 #2，", "然后 #3"],
  "deliveredB": ["order-2：", "先 #1，再 #2"],
  "redelivery": ["重新投递 order-1 的 #2 时，#3 也会回来，", "即使 #3 已经确认"],
  "inOrder": "按键的顺序"
};

# 排序

共享同一排序键、在同一区域发布的消息，按发布顺序到达启用了消息排序的订阅。

<Diagram name="ordering" words={words} />

顺序只在每个键内部成立，在其他任何地方都不成立。不同键的消息，以及不带键的消息，以任意顺序到达。排序由订阅决定：创建订阅时设定，此后不能更改。未启用排序的订阅以任意顺序投递带键的消息。

客户端发布者为每个键保持一批在途，因此一个键的消息按应用发布的顺序到达服务。不带键的消息并发发送。发布失败会暂停它的键：客户端拒绝该键后续的消息，直到应用恢复这个键。

服务在一个区域内对收到的消息排序。在多个区域发布同一个键的发布者之间没有顺序，区域端点（[`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint)）让它们留在同一个区域。订阅者可以从任何地方连接。

同一个键的消息通常交给订阅的同一个订阅者，但服务可能把键转给另一个订阅者。一个键最长 1 KB，每个键的发布速率最高 1 MBps。订阅上有死信政策时，顺序可能不成立，因为转入死信是尽力而为的。

## 在本 crate 中 {/* #in-this-crate */}

发布先从这次调用的 [`ordering_key`](api:ruststream_gcp_pubsub::PubSubOrdering::ordering_key) 步骤取键，其次从 `partition-key` 消息头取，最后取 [`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish::ordering_key) 策略在挂载点固定的键。键写入消息的排序键，绝不写成属性。带键的发布失败时，本 crate 恢复这个键并返回错误，因此该键下的下一次发布照常发送。

投递把它的键报告为 `partition-key` 消息头。按键分配的 worker（[`workers(n, by_key)`](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/workers/)）对它做哈希，因此一个键在服务内部也保持顺序。本 crate 通过 [`create_with_topic`](api:ruststream_gcp_pubsub::GooglePubSub::create_with_topic) 创建的订阅启用了消息排序。本 crate 找到的已有订阅保留它自己的设置。

规则来自 Google（[对消息排序](https://docs.cloud.google.com/pubsub/docs/ordering)）；发布者的规则来自 `google-cloud-pubsub` 1.3 客户端（[`Publisher`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/client/struct.Publisher.html)）。
