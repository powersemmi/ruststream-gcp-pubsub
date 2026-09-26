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

## Что даёт крейт {#what-the-crate-gives-you}

Один [дескриптор подписки](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#subscribing)
несёт все настройки подписки: управление потоком, на сколько продлевается срок подтверждения,
сколько ждёт неполный пакет и как долго одна доставка может оставаться неурегулированной. Предел
попыток и тема dead-letter, названные в точке монтирования, становятся собственной политикой
dead-letter этой подписки, поэтому сервис не публикует копий для повторной доставки.
[Публикация](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#publishing)
добавляет одну настройку на сообщение - ключ упорядочивания, - и назвать его может место вызова,
заголовок с ключом партиционирования или точка монтирования. С возможностью
[`testing`](https://docs.rs/ruststream-gcp-pubsub/latest/ruststream_gcp_pubsub/index.html#testing)
тест запускает то же приложение, что и `main`: `PubSubBroker` подключается внутри процесса, без
сервера.

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-language-rust: **[Справочник крейта](https://docs.rs/ruststream-gcp-pubsub)** - подписка, публикация, прелюдия, документ `AsyncAPI`, тестирование, эксплуатация.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - установка, учебник, список брокеров.
- :material-transit-connection-variant: **[Справочник фреймворка](https://docs.rs/ruststream/latest/ruststream/runtime/index.html)** - как писать подписчиков, маршрутизация, кодеки, middleware, CLI.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт - входная страница брокера Pub/Sub. Всё, что даёт сам брокер, лежит на
[docs.rs](https://docs.rs/ruststream-gcp-pubsub), а фреймворк описан в своём крейте и на
[сайте RustStream](https://powersemmi.github.io/ruststream/).
