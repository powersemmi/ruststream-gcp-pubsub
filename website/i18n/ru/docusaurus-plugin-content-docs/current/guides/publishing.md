---
title: Публикация и ответы
sidebar_position: 3
description: "Публикация в темы Pub/Sub из сервиса: ответы, тип которых называет тему, слоты Out в обработчике и сообщение при запуске."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Публикация и ответы

Это руководство публикует в темы Pub/Sub тремя способами, которыми это делает сервис: ответом на сообщение, публикацией из обработчика и сообщением после запуска сервиса.

Каждая публикация проходит через политику публикации.
[`Publish`](../components/publishing/index.md), имя [`PubSubPublish`](api:ruststream_gcp_pubsub::PubSubPublish) в прелюдии, публикует в тему и ждёт, пока Pub/Sub примет сообщение.
Публикация в несуществующую тему возвращает ошибку: Pub/Sub не хранит сообщения для темы, которой у него нет.

Пример - сервис заказов. Он отвечает на каждый заказ квитанцией, выставляет по нему счёт и сообщает о своём запуске.

## Ответ

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#reply"
```

</TabItem>
</Tabs>

Аргумент `reply` делает возвращаемое значение сообщением для публикации.
Куда оно уходит, определяет его тип: `#[outgoing(name = "receipts")]` отправляет каждый `Receipt` в тему `receipts`.
Квитанция публикуется после возврата из обработчика и до подтверждения заказа. Если квитанцию опубликовать не удалось, заказ остаётся неподтверждённым и приходит снова.

## Публикация из обработчика

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#slot"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#slot"
```

</TabItem>
</Tabs>

Параметр `Out` - издатель, которого обработчик вызывает сам.
`bill` называет тему в вызове через `.to("invoices")` и сам решает, что неудачная публикация значит для заказа.
Здесь он запрашивает заказ снова через `retry`, поэтому счёт не теряется.
До завершения доставки обработчик может опубликовать сколько угодно сообщений в любые темы.

## Точка монтирования

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish::default())` задаёт политику ответа, а `.out(DefaultSlot, Publish::default())` - отдельную политику параметра `Out`.
Политика хранит настройки для каждого сообщения своей позиции, например [`Publish::default().ordering_key(..)`](../components/publishing/ordering/policy.md).
Брокер создаёт один клиентский издатель на тему при первой публикации в неё, и все следующие публикации в эту тему используют его.

`after_startup` выполняется один раз, когда открыты все подписки.
Он получает работающего издателя той политики, которую называет, здесь чтобы сообщить о сервисе в `services`.
Если хук возвращает ошибку, запуск прерывается, поэтому сервис, который не может сообщить о себе, не работает.

## Запуск {/* #running-it */}

Темы, в которые публикует сервис, принадлежат тем, кто их читает, поэтому они существуют до запуска сервиса.
На эмуляторе создайте их через REST API:

```bash
for topic in receipts invoices services; do
  curl -X PUT localhost:8085/v1/projects/my-project/topics/$topic
done
cargo run -- run
```

Без `services` сообщение о запуске возвращает ошибку, и сервис не запускается.

## Тестирование

Тесты в файле примера публикуют заказ и через `published` проверяют, что дошло до `receipts` и `invoices`.
Второй тест запускает приложение и находит сообщение о запуске в `services`.
На внутрипроцессном Pub/Sub каждая публикация записывается, читает её тему какая-либо подписка или нет.
Работу с обвязкой описывает руководство [Тестирование сервиса на Pub/Sub](testing.md).
