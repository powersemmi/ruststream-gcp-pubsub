---
title: 重新投递
sidebar_position: 3
description: "nack 之后或截止时间届满后，Pub/Sub 再次投递消息，直到死信政策把它转到死信主题。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Pub/Sub 上的一次投递：ack 移除消息，nack 或截止时间届满让它回来，直到达到死信上限",
  "text": "以文字阅读本图",
  "delivery": ["一条消息的一次投递", "delivery_attempt: n"],
  "ack": ["ack：", "从订阅中", "移除"],
  "nack": ["nack，或", "截止时间届满"],
  "policy": ["死信政策：", "n 是否低于", "maxDeliveryAttempts？"],
  "deadLetter": ["达到上限：", "发布到", "死信主题"],
  "redeliver": ["低于上限或", "没有政策：", "按重试政策等待，", "然后回来"],
  "approximate": ["尝试次数是近似值，", "可能重置为零"],
  "below": "低于上限",
  "atCap": "达到上限"
};

# 重新投递

nack 之后或截止时间届满后，Pub/Sub 再次投递消息，直到死信政策把它转到死信主题。

<Diagram name="redelivery" words={words} />

订阅默认立即重新投递。它的重试政策可以改为指数退避，范围在 0 到 600 秒之间（默认 10 到 600 秒）。退避是尽力而为的：nack 很多时，消息等待的时间可能更短。

死信政策属于订阅，由一个死信主题和 `maxDeliveryAttempts` 组成，后者取值 5 到 100（默认 5）。只有配置了死信政策的订阅才统计尝试次数，也只有这时投递才带有 `delivery_attempt`。转发是尽力而为的，因此一条消息的投递次数可能比上限多几次。

服务转移消息的方式是：把它发布到死信主题，并在订阅中确认它。为此，Pub/Sub 的服务代理需要该主题的发布者角色和该订阅的订阅者角色。没有自己订阅的死信主题会丢失收到的消息。

## 精确一次投递 {/* #exactly-once-delivery */}

精确一次投递是订阅的另一项设置，适用于拉取和流式拉取。服务确认过的 ack 是最终的：消息不会再次投递。客户端必须请求这一确认。这一保证只对在同一区域连接的订阅者成立，区域端点可以确保这一点。它的投递明显比标准订阅慢。

## 在本 crate 中 {/* #in-this-crate */}

投递把 `delivery_attempt` 报告为 `pubsub-delivery-attempt` 消息头。注册上的 [`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) 和 [`dead_letter(topic)`](api:ruststream::runtime::Mounting::dead_letter) 成为订阅的死信政策，在订阅打开时写入。上限不在 5 到 100 之间，或者两者只声明了一个，启动就会在调用服务之前失败。每种处理器结果如何体现在这张图上，见[结算](../settlement.md)。

规则来自 Google（[重试政策](https://docs.cloud.google.com/pubsub/docs/subscription-retry-policy)、[死信主题](https://docs.cloud.google.com/pubsub/docs/handling-failures)、[精确一次投递](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery)）。
