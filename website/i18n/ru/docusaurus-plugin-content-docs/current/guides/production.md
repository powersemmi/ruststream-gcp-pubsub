---
title: Работа в промышленной среде
sidebar_position: 8
description: "Сервис на Pub/Sub в Google Cloud: учётные данные, региональная конечная точка, топология эксплуатации, роли IAM и exactly-once в регионе."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Работа в промышленной среде

Это руководство переводит сервис с эмулятора в Google Cloud: как он входит в систему, к какой конечной точке обращается, кто владеет топологией и какие роли ему нужны.

Пример - платёжный сервис, который берёт конфигурацию из окружения.

## Учётные данные и конечная точка

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

Без [`emulator`](../components/broker/emulator.md) брокер обращается к Google Cloud.
Он входит через Application Default Credentials, поэтому код не называет учётных данных.
В GKE, Cloud Run и Compute Engine они приходят с сервера метаданных: сервис работает от имени служебного аккаунта своей нагрузки.
На машине разработчика их записывает `gcloud auth application-default login`, а в любом другом месте `GOOGLE_APPLICATION_CREDENTIALS` называет файл ключа.
Сервис, который должен входить от имени определённого аккаунта, строит учётные данные через `google-cloud-auth` и передаёт их в [`credentials`](../components/broker/credentials.md).
Клиенты запрашивают токен при первом обращении к Pub/Sub, поэтому неверные учётные данные проявляются как ошибка этого обращения: вызова топологии при запуске, потока подписки или публикации.

Глобальная конечная точка направляет каждый запрос в ближайший регион.
[`endpoint`](../components/broker/endpoint.md) закрепляет сервис за одним регионом, здесь из `PUBSUB_ENDPOINT`.
Это нужно двум гарантиям: сообщения одного [ключа упорядочивания](ordering.md) приходят по порядку, только если опубликованы в одном регионе, а подписка exactly-once доставляет ровно один раз только подписчикам в своём регионе.
Конечная точка действует для всех клиентов брокера, включая вызовы топологии при запуске.

## Сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

В промышленной среде темы и подписки обычно относятся к инфраструктуре: их создают Terraform или `gcloud` со сроком хранения, фильтрами и политикой повторных попыток.
Тогда сервис называет свою подписку, `#[subscriber("payments-workers")]`, и ничего не создаёт.
Объявление dead-letter по-прежнему действует: при запуске сервис записывает его в подписку как её политику dead-letter.
Чтобы политика оставалась и в инфраструктуре, эксплуатация задаёт тот же предел и ту же тему, и обновление при запуске ничего не меняет.
[`create_with_topic`](../components/subscriptions/create-with-topic.md) подходит сервису, который сам владеет своей топологией, и эмулятору.

Запуск прерывается на первом вызове топологии, в котором Pub/Sub отказал, с ошибкой [`PubSubError::Admin`](api:ruststream_gcp_pubsub::PubSubError::Admin), которая называет ресурс.
Поток отсутствующей подписки возвращает ошибку [`PubSubError::Receive`](api:ruststream_gcp_pubsub::PubSubError::Receive).

## Роли

Служебному аккаунту сервиса нужны такие права:

| Сервис | Права | Предопределённая роль |
|---|---|---|
| читает подписку | `pubsub.subscriptions.consume` на ней | `roles/pubsub.subscriber` |
| публикует в тему | `pubsub.topics.publish` на ней | `roles/pubsub.publisher` |
| объявляет политику dead-letter на подписке | `pubsub.subscriptions.update` на ней | `roles/pubsub.editor` |
| использует `create_with_topic` | `pubsub.topics.get`, `pubsub.topics.create`, `pubsub.topics.attachSubscription`, `pubsub.subscriptions.get`, `pubsub.subscriptions.create` в проекте | `roles/pubsub.editor` |

Политике dead-letter нужен ещё служебный агент Pub/Sub проекта, `service-{project-number}@gcp-sa-pubsub.iam.gserviceaccount.com`.
Он публикует в тему dead-letter, поэтому ему нужна роль `roles/pubsub.publisher` на ней. Он подтверждает перенесённое сообщение, поэтому ему нужна роль `roles/pubsub.subscriber` на исходной подписке.

## Доставка exactly-once

Exactly-once - настройка подписки, которую задаёт эксплуатация при её создании.
Крейту настройка не нужна: на такой подписке каждое подтверждение ждёт ответа Pub/Sub, а подтверждение, которое Pub/Sub отклонил, возвращает ошибку с указанием причины.
Сервис подключается через региональную конечную точку региона подписки, потому что гарантия действует только там.
Что меняется для обработчика, описывает руководство [Подтверждение и переобработка](retries.md#exactly-once-subscriptions).

## Тестирование

Тесты запускают тот же сервис с собственным `Config` на внутрипроцессном Pub/Sub, где соединения не устанавливаются и учётные данные не читаются.
`app` читает окружение, поэтому без `GOOGLE_CLOUD_PROJECT` `main` сразу возвращает ошибку, ещё до любого соединения.
