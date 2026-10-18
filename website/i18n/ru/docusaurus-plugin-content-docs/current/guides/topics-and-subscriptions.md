---
title: Темы и подписки
sidebar_position: 2
description: "Чтение тем Pub/Sub через подписки: своя подписка у каждого потребителя, общая у реплик, созданная сервисом или эксплуатацией."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Темы и подписки

Это руководство читает одну тему из нескольких частей сервиса через подписки, которые создаёт сам сервис, и через подписки, которыми владеет эксплуатация.

Издатель отправляет сообщение в тему.
Подписка - самостоятельный ресурс, привязанный к одной теме. Она хранит каждое сообщение, опубликованное в тему после её создания.
Каждая подписка на тему получает каждое сообщение.
Потребители одной подписки делят её сообщения между собой.
Сервис всегда читает подписку.

Пример - сервис заказов: тему `orders` читают биллинг, доставка, аудит и отчёт.

## Своя подписка у каждого потребителя

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#fan-out"
```

</TabItem>
</Tabs>

У `bill` и `ship` своя подписка на `orders`, поэтому каждый получает каждый заказ.
Новый потребитель темы - это новая подписка. Для издателя ничего не меняется.
Подписка получает только то, что опубликовано после её создания, поэтому потребитель, добавленный позже, не видит более ранних заказов.

[`GooglePubSub`](../components/subscriptions/google-pubsub.md) называет подписку коротким идентификатором в проекте брокера или полным именем ресурса `projects/{project}/subscriptions/{id}`.
С [`create_with_topic`](../components/subscriptions/create-with-topic.md) топологией владеет сам сервис: при запуске он создаёт тему и подписку, если их нет, и не трогает существующие.
Подписка, которую он создаёт, включает упорядочивание сообщений. Его использует руководство [Ключи упорядочивания](ordering.md).

## Подписки, которыми владеет эксплуатация

В промышленной среде топология часто относится к инфраструктуре: её создают Terraform или `gcloud` до запуска сервиса.
Тогда сервис только называет то, что читает.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#existing"
```

</TabItem>
</Tabs>

`#[subscriber("orders-audit")]` читает [подписку](../components/subscriptions/index.md) `orders-audit` такой, какая она есть.
При запуске ничего не создаётся, а поток отсутствующей подписки возвращает ошибку [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).
`#[subscriber(GooglePubSub)]` фиксирует вид подписки и оставляет имя открытым.
Точка монтирования называет подписку через [`.name(..)`](../components/subscriptions/name.md), поэтому имя выбирает развёртывание, здесь из `REPORTS_SUBSCRIPTION`.
В ручной форме дескриптор строится там, где монтируется обработчик, поэтому принимает имя сразу.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/topics_and_subscriptions.rs#app"
```

</TabItem>
</Tabs>

На эмуляторе создайте обе подписки до запуска сервиса, как это сделала бы эксплуатация:

```bash
for subscription in orders-audit orders-reports; do
  curl -X PUT localhost:8085/v1/projects/my-project/subscriptions/$subscription \
    -H 'content-type: application/json' \
    -d '{"topic": "projects/my-project/topics/orders"}'
done
```

Тема `orders` существует, если сервис запускался хотя бы раз, потому что её создают биллинг и доставка.

## Несколько реплик сервиса

Каждая реплика сервиса открывает streaming pull на одних и тех же подписках.
Pub/Sub передаёт каждое сообщение подписки одной из них, поэтому реплики делят работу.
Чтобы масштабировать сервис, запустите больше реплик. Настройки не меняются.
Сообщение, которое одна реплика вернула через `retry`, может прийти к другой.

Внутри одной реплики подписка обрабатывает по одному сообщению.
`workers(n)` в атрибуте `#[subscriber]` обрабатывает до `n` сообщений одновременно. Форму с ключом показывает руководство [Ключи упорядочивания](ordering.md).

## Тестирование

Тесты в файле примера публикуют заказ в `orders` и проверяют, что его обработали и биллинг, и доставка.
Внутри процесса подписка, которую сервис только называет, читает одноимённую тему, поэтому тест `orders-audit` и `orders-reports` публикует туда.
Что моделирует внутрипроцессный Pub/Sub, описывает руководство [Тестирование сервиса на Pub/Sub](testing.md).
