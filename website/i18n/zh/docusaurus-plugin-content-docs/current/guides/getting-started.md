---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-gcp-pubsub，启动 Pub/Sub 模拟器，在模拟器上运行第一个订阅者，并在没有模拟器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-gcp-pubsub`，启动 Pub/Sub 模拟器，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-gcp-pubsub = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-gcp-pubsub` 是 Pub/Sub Broker。
它的 `testing` feature 加入测试所用的[进程内 Pub/Sub](../components/testing/index.md)。

这个 Broker crate 有两个 feature，默认都不启用：

| Feature | 提供的内容 |
|---|---|
| `testing` | `TestApp` 背后的进程内 Pub/Sub |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [Pub/Sub 绑定](../components/asyncapi/index.md) |

底层客户端 [google-cloud-pubsub](https://docs.rs/google-cloud-pubsub) 用 Rust 编写，因此构建不需要任何系统库。
本 crate 需要 Rust 1.95 或更高版本。

## 本地模拟器 {/* #a-local-emulator */}

Google 在 Cloud SDK 镜像中提供 Pub/Sub 模拟器：

```yaml title="compose.yaml"
services:
  pubsub:
    image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
    command: gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=my-project
    ports:
      - "8085:8085"
```

```bash
docker compose up -d
```

模拟器监听 `localhost:8085`。
它启动时是空的，接受任何项目 id。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从订阅 `orders-workers` 读取 JSON 订单。
它接下带金额的订单，丢弃没有金额的订单。
Pub/Sub 服务读取的总是订阅，而不是主题。
[`GooglePubSub`](../components/subscriptions/google-pubsub.md) 描述这个订阅，[`create_with_topic`](../components/subscriptions/create-with-topic.md) 在启动时把它创建在主题 `orders` 上，主题本身也一并创建。
[`PubSubBroker::new`](../components/broker/index.md) 接收项目 id，只把它记录下来。
[`emulator`](../components/broker/emulator.md) 让 Broker 连接本地模拟器，因为 Rust 客户端不读取 `PUBSUB_EMULATOR_HOST`。
连接在服务启动时才打开。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，通过模拟器的 REST API 发布一个订单：

```bash
curl -X POST localhost:8085/v1/projects/my-project/topics/orders:publish \
  -H 'content-type: application/json' \
  -d "{\"messages\": [{\"data\": \"$(printf '{"id":1,"amount_cents":1250}' | base64)\"}]}"
```

Pub/Sub 的 REST API 以 base64 传递消息体，所以订单要先编码。
服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`PubSubBroker` 连接的是进程内 Pub/Sub，所以测试不需要模拟器：

```bash
cargo test
```

[测试 Pub/Sub 服务](testing.md)逐一讲解这些测试，以及进程内 Pub/Sub 模拟了什么。

## 从 google-cloud-pubsub 迁移 {/* #coming-from-google-cloud-pubsub */}

基于 google-cloud-pubsub 的服务要自己构建客户端，在循环中拉取消息，并手动发布。
下表列出每一部分在 RustStream 中的位置：

| google-cloud-pubsub | RustStream |
|---|---|
| 带 `with_endpoint` 和 `with_credentials` 的 `Subscriber::builder()` | [`PubSubBroker::new`](../components/broker/index.md)，加上 [`endpoint`](../components/broker/endpoint.md)、[`credentials`](../components/broker/credentials.md) 或 [`emulator`](../components/broker/emulator.md) |
| `subscriber.subscribe(name)` 加一个遍历消息流的循环 | 一个 `#[subscriber(..)]` 处理器 |
| `set_max_outstanding_messages`、`set_max_lease_extension`、`set_max_lease` | 描述符上的 [`max_outstanding`](../components/subscriptions/max-outstanding.md)、[`ack_extension`](../components/subscriptions/ack-extension.md)、[`max_lease`](../components/subscriptions/max-lease.md)（[流控与租约](flow-control.md)） |
| 管理客户端上的 `create_topic` 和 `create_subscription` | [`create_with_topic`](../components/subscriptions/create-with-topic.md)（[主题与订阅](topics-and-subscriptions.md)） |
| 对消息数据调用 `serde_json::from_slice` | 处理器的参数类型，由编解码器解码 |
| `handler.ack()`、`handler.nack()` | 处理器的结果：`ack`、`retry`、`retry_after`、`drop`（[确认与重试](retries.md)） |
| `Publisher::publish` | 应答或 `Out` 参数，经由 [`Publish`](../components/publishing/index.md) 策略发送（[发布与应答](publishing.md)） |
| `Message::set_ordering_key` | [`ordering_key`](../components/publishing/ordering/ordering-key.md) 步骤（[排序键](ordering.md)） |
