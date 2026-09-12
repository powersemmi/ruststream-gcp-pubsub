# ruststream-gcp-pubsub

**`ruststream-gcp-pubsub`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/)
на Google Cloud Pub/Sub поверх официального клиента
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub).

Поток сообщений, который читает обработчик, - это подписка в режиме streaming pull. Подтверждение
нативное и отдельное для каждого сообщения. На подписке exactly-once ack идёт в подтверждаемой
форме. Ключ упорядочивания - это ключ партиционирования фреймворка.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-gcp-pubsub = "0.7"
serde = { version = "1", features = ["derive"] }
```

Крейт опубликован на crates.io и следует линейке `ruststream` 0.7. Его MSRV - 1.88, и его задаёт
официальный клиент.

Сервис на Pub/Sub вы собираете в функции приложения:

```rust
--8<-- "crates/ruststream-gcp-pubsub/examples/pubsub_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Руководство по Pub/Sub](pubsub.md)** - подписки, подтверждение, ключи упорядочивания, эмулятор и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-gcp-pubsub)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт документирует только брокер Pub/Sub. Как писать подписчиков, публиковать сообщения,
настраивать маршрутизацию, кодеки, middleware, наблюдаемость и CLI, объясняет
[документация RustStream](https://powersemmi.github.io/ruststream/).
