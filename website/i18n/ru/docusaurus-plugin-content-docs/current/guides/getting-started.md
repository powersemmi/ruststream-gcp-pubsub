---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-gcp-pubsub, запуск эмулятора Pub/Sub, первый подписчик на нём и его тест без эмулятора."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-gcp-pubsub`, запускает эмулятор Pub/Sub и первый сервис на нём.

## Установка

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

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-gcp-pubsub` - брокер Pub/Sub.
Его фича `testing` добавляет [внутрипроцессный Pub/Sub](../components/testing/index.md), на котором работают тесты.

У крейта брокера две фичи, обе выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `testing` | внутрипроцессный Pub/Sub, на котором работает `TestApp` |
| `asyncapi` | [привязки Pub/Sub](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |

Клиент под ним - [google-cloud-pubsub](https://docs.rs/google-cloud-pubsub). Он написан на Rust, поэтому сборке не нужны системные библиотеки.
Крейту нужен Rust 1.95 или новее.

## Локальный эмулятор

Google поставляет эмулятор Pub/Sub в образе Cloud SDK:

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

Эмулятор слушает `localhost:8085`.
Он запускается пустым и принимает любой идентификатор проекта.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из подписки `orders-workers`.
Заказ с суммой он принимает, а заказ без суммы отбрасывает.
Сервис на Pub/Sub всегда читает подписку, а не тему.
[`GooglePubSub`](../components/subscriptions/google-pubsub.md) описывает подписку, а [`create_with_topic`](../components/subscriptions/create-with-topic.md) при запуске создаёт её на теме `orders` вместе с самой темой.
[`PubSubBroker::new`](../components/broker/index.md) принимает идентификатор проекта и только запоминает его.
[`emulator`](../components/broker/emulator.md) направляет брокер на локальный эмулятор, потому что клиент на Rust не читает `PUBSUB_EMULATOR_HOST`.
Соединение открывается при запуске сервиса.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте заказ из другого терминала через REST API эмулятора:

```bash
curl -X POST localhost:8085/v1/projects/my-project/topics/orders:publish \
  -H 'content-type: application/json' \
  -d "{\"messages\": [{\"data\": \"$(printf '{"id":1,"amount_cents":1250}' | base64)\"}]}"
```

В REST API Pub/Sub передаёт тело сообщения в base64, поэтому заказ сначала кодируется.
Сервис пишет в журнал `order taken` с номером заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `PubSubBroker` подключается к внутрипроцессному Pub/Sub, поэтому тестам не нужен эмулятор:

```bash
cargo test
```

Руководство [Тестирование сервиса на Pub/Sub](testing.md) разбирает такие тесты и то, что моделирует внутрипроцессный Pub/Sub.

## Переход с google-cloud-pubsub

Сервис на google-cloud-pubsub сам строит клиенты, читает сообщения в цикле и публикует вручную.
Вот куда переходит каждая часть в RustStream:

| google-cloud-pubsub | RustStream |
|---|---|
| `Subscriber::builder()` с `with_endpoint` и `with_credentials` | [`PubSubBroker::new`](../components/broker/index.md) с [`endpoint`](../components/broker/endpoint.md), [`credentials`](../components/broker/credentials.md) или [`emulator`](../components/broker/emulator.md) |
| `subscriber.subscribe(name)` и цикл по потоку сообщений | обработчик `#[subscriber(..)]` |
| `set_max_outstanding_messages`, `set_max_lease_extension`, `set_max_lease` | [`max_outstanding`](../components/subscriptions/max-outstanding.md), [`ack_extension`](../components/subscriptions/ack-extension.md), [`max_lease`](../components/subscriptions/max-lease.md) на дескрипторе ([Управление потоком и аренда](flow-control.md)) |
| `create_topic` и `create_subscription` на административных клиентах | [`create_with_topic`](../components/subscriptions/create-with-topic.md) ([Темы и подписки](topics-and-subscriptions.md)) |
| `serde_json::from_slice` над данными сообщения | тип параметра обработчика, который декодирует кодек |
| `handler.ack()`, `handler.nack()` | исход обработчика: `ack`, `retry`, `retry_after`, `drop` ([Подтверждение и переобработка](retries.md)) |
| `Publisher::publish` | ответ или параметр `Out`, отправленный через политику [`Publish`](../components/publishing/index.md) ([Публикация и ответы](publishing.md)) |
| `Message::set_ordering_key` | шаг [`ordering_key`](../components/publishing/ordering/ordering-key.md) ([Ключи упорядочивания](ordering.md)) |
