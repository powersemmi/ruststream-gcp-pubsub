---
title: Устройство
sidebar_position: 2
description: "Где стоит ruststream-gcp-pubsub: между рантаймом RustStream и клиентами google-cloud-pubsub, один streaming pull на подписку, один издатель на тему."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Сверху вниз: сервис, рантайм RustStream, этот крейт, клиенты google-cloud-pubsub и Pub/Sub",
  "text": "Схема текстом",
  "service": "Ваш сервис",
  "handlers": ["#[subscriber]", "обработчики"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "Ядро RustStream",
  "runtime": "Рантайм: диспетчеризация, воркеры, завершение",
  "crate": "ruststream-gcp-pubsub",
  "subscriber": ["PubSubSubscriber", "задача перекачки", "на подписку"],
  "publishers": ["PubSubPublisher", "издатель клиента", "на тему"],
  "message": ["PubSubMessage", "держит аренду"],
  "broker": ["PubSubBroker", "конфигурация"],
  "descriptor": ["GooglePubSub", "подписка"],
  "inProcess": ["Внутрипроцессный", "транспорт (testing)"],
  "client": "google-cloud-pubsub",
  "pull": ["Subscriber", "streaming pull"],
  "clients": ["Publisher,", "TopicAdmin, ..."],
  "pubsub": "Pub/Sub или эмулятор",
  "mounted": "монтируются",
  "polls": "опрашивает поток"
};

# Устройство

Где стоит `ruststream-gcp-pubsub`: между рантаймом RustStream и клиентами `google-cloud-pubsub`,
один streaming pull на подписку, один издатель на тему.

<Diagram name="design" words={words} />

Крейт реализует трейты брокера из ядра поверх клиента Google
[`google-cloud-pubsub`](https://docs.rs/google-cloud-pubsub/1.3.0/google_cloud_pubsub/). Рантайм
над ним один для всех брокеров. Всё, что относится к Pub/Sub, лежит в этом крейте:
[дескриптор подписки](api:ruststream_gcp_pubsub::GooglePubSub), то, что каждый исход обработчика
делает с арендой, и то, как публикация передаёт свой ключ упорядочивания.

Подключение создаёт четыре клиента и делит их на всё время жизни соединения: `Subscriber`,
`BasePublisher`, `TopicAdmin` и `SubscriptionAdmin`. Административные клиенты создают топологию,
которую объявляет дескриптор, и записывают политику dead-letter, которую объявляет регистрация.
[`emulator`](api:ruststream_gcp_pubsub::PubSubBroker::emulator) направляет все клиенты на
локальный эмулятор с анонимными учётными данными. Иначе клиенты используют
[`endpoint`](api:ruststream_gcp_pubsub::PubSubBroker::endpoint) и
[`credentials`](api:ruststream_gcp_pubsub::PubSubBroker::credentials), если они заданы, а если нет,
учётные данные Application Default Credentials.

Каждая подписка запускает задачу перекачки. Она читает поток клиента и передаёт каждую доставку в
канал на 16 мест, который опрашивает рантайм. Пакеты собираются на этом канале. Аренда клиента
едет внутри каждого [`PubSubMessage`](api:ruststream_gcp_pubsub::PubSubMessage), поэтому
завершение доставки уходит прямо в клиент.

[Издатели](api:ruststream_gcp_pubsub::PubSubPublisher) одного соединения делят один издатель
клиента на тему, и создаёт его первая публикация в эту тему. Клиент собирает то, что они
отправляют, в пакеты.

Под фичей `testing` [`TestApp`](api:ruststream::testing::TestApp) подключает тот же брокер не к
клиентам, а к внутрипроцессному транспорту. Транспорт моделирует темы и подписки, конкурирующих
подписчиков, подтверждение и повторную доставку, счёт попыток и перенос в тему dead-letter. Сборка
без этой фичи не содержит ничего из этого.

Как части запускаются и останавливаются, показывает страница [Жизненный цикл](lifecycle.md), а
путь одного сообщения - страница [Путь](path.md).
