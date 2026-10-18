---
title: 批处理
sidebar_position: 7
description: "按批处理 Pub/Sub 消息：切片处理器、批的大小和等待时间，以及单独结算批中的每条消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 批处理

本指南一次把多条消息交给处理器，适用于按批做比逐条做更省的工作，例如向数据仓库的一次写入。

Pub/Sub 逐条投递消息，并单独结算每条消息。
服务在自己这一侧把一个订阅的投递收集成批，为这一批调用一次处理器，然后结算其中的每条消息。

示例把页面点击加载到分析数据仓库中。

## 批处理器 {/* #the-batch-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/batches.rs#load"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/batches.rs#load"
```

</TabItem>
</Tabs>

切片参数 `&[Click]` 让 `load` 成为[批处理器](../components/subscriptions/batches.md)。
描述符上的 [`batch_wait`](api:ruststream_gcp_pubsub::GooglePubSub::batch_wait) 是未满的批等待更多消息的时间，这里是半秒。
批一满就交出；等待结束时，批带着已有的消息交出。

`load` 为每次点击返回一个结果，顺序与批中一致。
来自数据仓库尚不认识的用户的点击会重试：Pub/Sub 再次投递这次点击，它单独到达，或进入后面的某一批。
没有页面的点击被丢弃，其余的得到确认。
单个 `HandlerOutcome` 以同样的方式结算整批。
在手写形式中，`Ok(())` 确认整批，`Err` 逐个点击携带结果。

## 挂载点 {/* #the-mount-site */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/batches.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/batches.rs#app"
```

</TabItem>
</Tabs>

`batch(nonzero!(100))` 是批的大小，批处理器没有它就无法编译。
批中的每条消息都持有租约，直到整批结算，所以批的大小要小于订阅的 [`max_outstanding`](../components/subscriptions/max-outstanding.md)，默认 1000。
死信声明单独统计每次点击：一次点击经过五次投递后转入 `clicks-dead`，不论它进过多少批。
死信主题的细节见[确认与重试](retries.md)。

## 测试 {/* #testing */}

测试用 `tokio::join!` 同时发布点击，使它们进入同一批，并用 `assert_batch_sizes` 和 `batches` 读回各批。
重试的点击表现为只有一条消息的批：先是两条的批，然后陌生用户的点击单独出现四次，之后它就在 `clicks-dead` 上了。
测试套件的用法见[测试 Pub/Sub 服务](testing.md)。
