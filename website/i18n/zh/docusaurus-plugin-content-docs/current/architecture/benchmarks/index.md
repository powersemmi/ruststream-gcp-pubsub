---
title: 基准测试
description: "ruststream-gcp-pubsub 在每条消息上的开销：与裸 google-cloud-pubsub 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {MovedSections, PubSubEnvironment, PubSubResults} from '@site/src/components/Benchmarks';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "rustc": "编译器", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "round_trip": "往返时间", "valgrind": "Valgrind"};

# 基准测试

`ruststream-gcp-pubsub` 在每条消息上的开销：与裸 `google-cloud-pubsub` 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在本地 Pub/Sub 模拟器上用三种方式跑每个场景。「裸客户端」直接驱动 `google-cloud-pubsub`。
「Broker crate」只驱动这个 crate，不含其上的任何东西：它的订阅、它交出的投递、它的发布者，由一个外面
没有服务的循环来读取和确认。「RustStream」是用户会写的服务：`#[subscriber]` 处理器、应用和运行时。
第一段差距是这个 crate 在它所封装的客户端之上的开销，这个数字由本仓库负责；第二段是运行时叠在上面的
开销，而且是在 Pub/Sub 上的那一份。

每个循环画成一条从最佳一轮到最慢一轮的横条，圆点标出最佳一轮，刻痕标出中位一轮。坐标轴的零点是裸客户端
的最佳一轮，圆点在零点右侧表示更慢。空心圆点表示该循环与裸客户端的差值小于各轮的波动范围。图下的表格
给出最佳一轮，括号里是中位一轮。

第二个场景带着排序键发布，这正是开启了排序的订阅必须按键保序的那种投递形态。它同时给这个 crate
多加了第一个场景没有的一份活：键被搬进投递的头里，不指名 Broker 的服务就从那里把它当作分区键读
出来。客户端上的循环读消息上的字段，什么也不复制。

## 代码的开销 {/* #cost-of-the-code */}

第二张图数一条消息在服务线程上花掉多少：指令数由 callgrind 统计，内存分配次数由 DHAT 统计，都是稳态下
的值。每个场景都是用户会写的那种服务：它建在 `PubSubBroker` 上，对着同一套环境里的模拟器启动。服务线程
上的一切都计入：框架的分发、本 crate 的代码，以及 `google-cloud-pubsub` 客户端，它的流式拉取、确认、
确认截止时间的延长和发布请求都在这里运行。「冷启动」是启动服务并处理第一次投递一次性付出的开销。

每一行的大头都是客户端。回复那一行最重：运行时要等每条回复得到确认，才去取下一次投递，所以每条回复
都单独作为一个发布请求发出。冷启动的开销也几乎全在客户端：它要构建两个管理客户端，每一个都会读取并
解析这台机器的根证书，所以这一列取决于机器上的证书库。

## 结果 {/* #results */}

<PubSubResults labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<PubSubEnvironment labels={machine} />

构建标志和数字一起公布，因为它们会改变这些数字。用 `-C target-cpu=native` 构建出的二进制给出的
结果，换一台机器就复现不了，所以这条 recipe 在构建前先把这个变量清空。

这些数字来自这个版本的 `docs/benchmarks/results.json`，由基准测试 recipe 写入。同一份文档公布在
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-gcp-pubsub/latest/benchmarks/results.json)，
框架的站点用它拼出跨 Broker 的汇总表。[方法论](methodology.md)说明这些数字怎样测得，
[数字](numbers.md)说明它们能支持哪些结论。

<MovedSections />
