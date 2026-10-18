---
title: 确认与重试
sidebar_position: 4
description: "结算 Pub/Sub 投递：确认、立即或延迟重试、丢弃，限制投递次数，并把一再失败的消息转到死信主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 确认与重试

本指南按处理器的结果结算每次 Pub/Sub 投递，立即或延迟重试，并用[死信主题](../components/settlement/dead-letters.md)限制投递次数。

Pub/Sub 单独结算每条消息。
得到确认的消息就此完成；交还的消息会再次投递，可能投递给另一个消费者。
订阅统计投递次数，并把用尽次数的消息转到它的死信主题。
本 crate 把处理器的结果映射为这些操作，把挂载点上的上限映射为订阅的死信政策。

示例是一个通过银行扣款的支付服务。

## 处理器 {/* #the-handler */}

银行对一次扣款有四种答复，每种需要不同的结果。
`Charged` 表示完成：`ack`。
`Held` 表示银行正在审核这笔付款，需要一段时间：[`retry_after`](../components/settlement/retry-after.md) 在 30 秒后再问一次。
`Unreachable` 表示银行没有应答：`retry` 立即再问一次。
`Declined` 表示卡已关闭，再试也是同样的答复，所以丢弃这笔付款。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#charge"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#charge"
```

</TabItem>
</Tabs>

在 Pub/Sub 上，这些结果变成以下调用：

| 结果 | Pub/Sub 的行为 |
|---|---|
| `ack` | 确认；消息不再投递 |
| `retry` | nack；订阅再次投递这条消息 |
| `retry_after(delay)` | 服务在租约下扣留消息，等待 delay，然后 nack |
| `drop` | 确认；在上限允许的最后一次投递上 nack，于是消息转入死信主题 |

没有重试政策的订阅会立即再次投递被 nack 的消息。
运维设置了指数退避政策的订阅会先等待，此时 `retry` 也会等待。

Pub/Sub 没有延迟 nack，所以 [`retry_after`](../components/settlement/retry-after.md) 把消息留在服务中。
在延迟期间，客户端继续延长它的租约，延迟结束时再 nack。
延迟必须在订阅的 [`max_lease`](../components/subscriptions/max-lease.md) 之内，默认一小时；更长的延迟会让消息立即回来。
服务在延迟期间停止时，不再延长租约，消息在租约到期时回来。
租约的细节见[流控与租约](flow-control.md)。

[`DELIVERY_ATTEMPT_HEADER`](../components/settlement/attempts.md) 是投递的尝试序号，第一次投递为 1。
Pub/Sub 只在有死信政策的订阅上统计尝试次数，而且这个计数是估计值，可能漏掉某次投递。

## 上限与死信主题 {/* #the-cap-and-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 和 [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) 成为订阅自己的死信政策，在启动时写入。
第五次投递之后，Pub/Sub 不再投递这笔付款，而是把它发布到 `payments-dead`，载荷和属性保持到达时的样子。
服务自己不发布任何东西，所以崩溃不会让这次转移丢失。
Pub/Sub 接受 5 到 100 的上限；其他值，或者只写了这对设置中的一半，都会让启动失败。
被丢弃的付款得到确认，不会去任何地方；死信主题只保存用尽投递次数的付款。

订阅 `payments-workers` 创建时带有这个政策，主题 `payments-dead` 也随之创建。
运维创建的订阅则在启动时被写入这个政策。
在生产环境中，项目的 Pub/Sub 服务账号需要向死信主题发布的权限，以及订阅源订阅的权限；所需角色列在[在生产环境中运行](production.md)一页。

## 读取死信主题 {/* #reading-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

死信主题是普通的主题，`review` 通过它自己的订阅读取它。
发布到没有订阅的主题的消息会丢失，所以这个订阅要在第一笔付款用尽投递次数之前存在。
这里由 `review` 在启动时创建它。

## 精确一次订阅 {/* #exactly-once-subscriptions */}

启用精确一次投递的订阅保证：得到确认的消息不会再次投递。
这是订阅的设置，在运维创建订阅时设定；服务不需要自己的设置。
本 crate 从每次投递中读取这一设置，在这样的订阅上，`ack` 和 `retry` 要等 Pub/Sub 确认。
Pub/Sub 拒绝的确认（例如租约到期之后）会让结算返回一个指明原因的错误，消息可能回来。
这一保证对订阅所在区域内的订阅者成立，所以服务通过该区域的[端点](../components/broker/endpoint.md)连接。

## 测试 {/* #testing */}

示例文件中的测试为每种答复发布一笔付款。
被扣留的付款只有在暂停的时钟上过了 30 秒后才回来。
银行无法访问时，付款花掉五次投递，然后 `review` 收到它。
被拒绝的付款只处理一次，永远不会进入审核。
测试套件的用法见[测试 Pub/Sub 服务](testing.md)。
