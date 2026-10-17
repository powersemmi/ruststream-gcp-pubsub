---
title: 生命周期
sidebar_position: 3
description: "PubSubBroker::new 只记录配置，connect 构建客户端，启动时打开每个订阅，关闭时刷出发布者。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 Pub/Sub 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "pubsub": "在 Pub/Sub 上",
  "unconnected": ["PubSubBroker::new", "(project)"],
  "connected": ["ConnectedPubSubBroker"],
  "closed": ["Closed: ()"],
  "config": ["只记录配置，", "无 I/O"],
  "clients": ["为连接构建", "四个客户端"],
  "subscriptions": ["每个订阅：", "有声明时先", "创建拓扑，", "再打开它的流"],
  "publisher": ["主题第一次发布时", "启动它的发布者"],
  "flush": ["先关闭，再刷出", "每个主题的", "发布者"],
  "afterShutdown": ["connect 之前或 shutdown 之后使用的发布者", "返回 NotConnected，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`PubSubBroker::new`](api:ruststream_gcp_pubsub::PubSubBroker::new) 只记录配置，connect 构建客户端，启动时打开每个订阅，关闭时刷出发布者。

<Diagram name="lifecycle" words={words} />

`new` 不做任何 I/O，所以服务的构建函数保持同步。connect 之前从 Broker 取得的发布者，共用运行时稍后填入的那个连接。

每个订阅在启动时打开一次。先检查它的描述符：名称为空、死信上限不在 5 到 100 之间，或者声明只写了一半，启动都会在调用服务之前失败。带 [`create_with_topic`](api:ruststream_gcp_pubsub::GooglePubSub::create_with_topic) 的描述符随后创建缺失的主题、死信主题和订阅，创建的订阅启用消息排序。连接 Pub/Sub 时，这需要创建主题和订阅的权限。订阅已经存在、又声明了死信政策时，该政策会写到这个订阅上。然后打开流式拉取。

直接用名称挂载的注册以同样的方式声明它的重试。同一订阅的两个注册如果声明不同，启动就会失败，因为一个订阅只有一个死信政策。

关闭先把连接标为已关闭，然后刷出每个主题的发布者，使分批的消息到达服务。关闭后的状态不包含任何数据。运行时丢弃订阅时，订阅停止：客户端不再读取，但继续延长仍被持有的消息的租约，直到它们结算。
