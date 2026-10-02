# ruststream-gcp-pubsub

**`ruststream-gcp-pubsub`** 在 Google Cloud Pub/Sub 上运行
[RustStream](https://powersemmi.github.io/ruststream/) 服务，底层是官方的
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub) 客户端。

处理器读取的消息流，就是一条流式拉取订阅。确认是原生的，而且逐条进行。在精确一次订阅上，ack
采用带确认的形式。排序键就是框架的分区键。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

该 crate 发布在 crates.io 上，跟随 `ruststream` 的 0.7 线。它的 MSRV 是 1.88，由官方客户端决定。

你在 app 函数里组装一个 Pub/Sub 服务：

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

## 这个 crate 给你什么 { #what-the-crate-gives-you }

一个[订阅描述符](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#subscribing)
带上了订阅的全部设置：流控、确认截止时间每次延长多久、未满的批还要等多久，以及一条投递最长可以
多久不被结算。在挂载点写下的重试上限和死信主题，会变成这条订阅自己的死信策略，因此服务不会再发
布任何用于重新投递的副本。[发布](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#publishing)
只多出一项逐条消息的设置，也就是排序键，它可以由调用处、分区键请求头或挂载点给出。
启用 [`testing`](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#testing)
特性后，测试运行的就是 `main` 所用的同一个应用：`PubSubBroker` 在进程内连接，不需要服务器。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-language-rust: **[crate 参考](https://docs.rs/ruststream-gcp-pubsub)** - 订阅、发布、prelude、`AsyncAPI` 文档、测试与运维。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 安装、教程和 Broker 列表。
- :material-transit-connection-variant: **[框架参考](https://docs.rs/ruststream/latest/ruststream/runtime/index.html)** - 编写订阅者、路由、编解码器、中间件和 CLI。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点是 Pub/Sub Broker 的入口页。Broker 自身提供的一切都在
[docs.rs](https://docs.rs/ruststream-gcp-pubsub) 上，框架则由它自己的 crate 和
[RustStream 站点](https://powersemmi.github.io/ruststream/)介绍。
