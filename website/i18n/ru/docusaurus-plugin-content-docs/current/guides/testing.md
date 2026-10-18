---
title: Тестирование сервиса на Pub/Sub
sidebar_position: 9
description: "Тестирование сервиса на Pub/Sub с TestApp: рабочее приложение внутри процесса, ответы, отбрасывание, задержки, dead-letter и тест на эмуляторе."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на Pub/Sub

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [внутрипроцессном Pub/Sub](../components/testing/index.md) или на эмуляторе.

Тестируемый сервис проводит возвраты.
Возврат приходит в `refunds`, и обработчик отвечает на него сообщением `Refunded` в `refunds-done`.
Возврат без суммы отбрасывается.
Возврат сверх лимита согласования ждёт согласования минуту с [`retry_after`](../components/settlement/retry-after.md).
После пяти доставок возврат уходит в `refunds-review`, [тему dead-letter](../components/settlement/dead-letters.md).
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт брокер для эмулятора.
Тестам нужна фича `testing` крейта `ruststream-gcp-pubsub` в `[dev-dependencies]`.

## Возврат с ответом

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#answered"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#answered"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`PubSubBroker`](../components/broker/index.md) к внутрипроцессному Pub/Sub.
Соединения не устанавливаются, и учётные данные не читаются.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<PubSubBroker>()`.
`Refund` называет свою тему, поэтому `message(..).publish()` не нужен адрес.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `refunds-done`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на эмуляторе ниже.

## Отброшенный возврат

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что ответ не опубликован.

## Задержка без ожидания

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `APPROVAL_WAIT` не стоит ничего.
Каждое ожидание заканчивается повторной доставкой того же сообщения, и `assert_called` их считает.
Пятая доставка тоже ждёт, а когда её ожидание заканчивается, Pub/Sub переносит возврат в `refunds-review` в том виде, в каком он пришёл.
`published` читает то, что дошло до темы, кто бы его туда ни поместил: сервис или Pub/Sub.

## Что моделирует внутрипроцессный Pub/Sub

Внутрипроцессный Pub/Sub читает проект рабочего брокера и каждый дескриптор, который монтирует сервис.
Публикация, подписка или объявление, которые отклонил бы Pub/Sub, так же прерывают тест.
Он моделирует Pub/Sub, а не очередь:

- Темы и подписки разделены, и каждая подписка на тему получает каждое сообщение, опубликованное после её создания.
- Потребители одной подписки делят её сообщения между собой.
- `retry` доставляет сообщение снова, `retry_after` сначала ждёт на часах теста, а `drop` подтверждает.
- Политика dead-letter считает доставки и переносит сообщение, у которого они кончились.

Топология, которую сервис ожидает найти, считается существующей.
Подписка, которую сервис только называет, читает одноимённую тему, а тема, в которую публикует сервис, принимает сообщение.
Для аренды, управления потоком, упорядоченной доставки по ключу и доставки exactly-once нужен эмулятор или сам Pub/Sub.

## Тот же тест на эмуляторе

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к эмулятору.
Тогда публикация ждёт до десяти секунд, пока её обработает каждая подписка, до которой она дошла. Другой срок задаёт `start_live_within`.
Тест читает адрес эмулятора из `PUBSUB_TEST_HOST` через `live::host`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
Эмулятор хранит подписки между запусками, поэтому `live::fresh` сначала удаляет `refunds-workers`, и запуск читает только то, что публикует сам.
`create_topic` создаёт `refunds-done` так, как это сделала бы эксплуатация, потому что ответ в несуществующую тему возвращает ошибку.
Проверки те же, что внутри процесса, без изменений.

```bash
cargo test
PUBSUB_TEST_HOST=localhost:8085 cargo test -- --test-threads=1
```
