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

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Pub/Sub 指南](pubsub.md)** - 订阅、确认、排序键、模拟器和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-gcp-pubsub)** - 该 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点只介绍 Pub/Sub Broker。编写订阅者、发布、路由、编解码器、中间件、可观测性和 CLI，都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
