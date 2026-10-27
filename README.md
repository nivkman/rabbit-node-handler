# Rabbit Node.js Handler 🐇

[![npm version](https://img.shields.io/npm/v/rabbit-node-handler)](https://www.npmjs.com/package/rabbit-node-handler)

[![npm version](https://img.shields.io/npm/v/rabbit-node-handler.svg)](https://www.npmjs.com/package/rabbit-node-handler)
[![npm downloads](https://img.shields.io/npm/dm/rabbit-node-handler.svg)](https://www.npmjs.com/package/rabbit-node-handler)
[![tests](https://github.com/nivkman/rabbit-node-handler/actions/workflows/test.yml/badge.svg)](https://github.com/nivkman/rabbit-node-handler/actions/workflows/test.yml)
[![license](https://img.shields.io/npm/l/rabbit-node-handler.svg)](./LICENSE)

A lightweight and easy-to-use RabbitMQ handler for Node.js applications. One call sets up a durable exchange, a consumer queue and a confirmed publisher — with automatic reconnection and graceful shutdown built in.

Same API as [`kafka-node-handler`](https://github.com/nivkman/kafka-node-handler), for RabbitMQ.

## ✨ Features

- 🔧 Easy setup with a single line of code
- 🔄 Automatic reconnection with exponential backoff (exchange, queue and consumer are re-declared for you)
- 📡 Event-based consumer: your callback gets `{ routingKey, event }`, parsed from JSON
- ✅ Acknowledgements handled: resolve to ack, throw to nack (drop, dead-letter or requeue — your choice)
- 📤 Simple publisher: `sendMessage(routingKey, event)` resolves when the broker **confirms** the message
- 👥 Kafka-style consumer groups: same `groupId` = share the work, different `groupId` = everyone gets a copy
- 🛑 Graceful shutdown on `SIGINT` / `SIGTERM` — stops consuming, lets in-flight messages finish and ack, then disconnects
- 📝 Timestamped logging via [`logger-standard`](https://www.npmjs.com/package/logger-standard), `.d.ts` types included

## 📦 Installation

```bash
npm install rabbit-node-handler
```

Requires Node.js 18+ and a RabbitMQ broker (`docker run -p 5672:5672 rabbitmq:4` is enough to try it).

## 🚀 Usage

### Initializing RabbitMQ

```javascript
import { initializeRabbit } from 'rabbit-node-handler';

await initializeRabbit({
  url: process.env.RABBITMQ_URL,        // amqp://user:pass@host:5672
  topic: process.env.TOPIC,             // exchange name
  groupId: process.env.GROUP_ID,        // queue name — this consumer group
  onNewMessage: handleNewMessage,
});

function handleNewMessage({ routingKey, event }) {
  console.log(`Received message with key ${routingKey}:`, event);
  // Process the message here. Resolve = ack. Throw = nack.
}
```

### Sending Messages 📨

```javascript
import { sendMessage } from 'rabbit-node-handler';

await sendMessage('user.created', { id: 7, name: 'Niv' });
```

### Routing, dead-lettering and retries

```javascript
await initializeRabbit({
  url: process.env.RABBITMQ_URL,
  topic: 'events',
  groupId: 'billing-service',
  routingKeys: ['order.*', 'payment.#'],          // only what this service cares about
  prefetch: 10,                                    // messages in flight per consumer
  queueOptions: { deadLetterExchange: 'events.dlx' }, // failed messages go here instead of being dropped
  onNewMessage: async ({ routingKey, event, headers, redelivered }) => {
    await chargeCustomer(event);
  },
});
```

### More than one exchange or connection per process

```javascript
import { RabbitHandler } from 'rabbit-node-handler';

const orders = await new RabbitHandler({ url, topic: 'orders', groupId: 'fulfilment', onNewMessage }).initialize();
const audit  = await new RabbitHandler({ url, topic: 'audit' }).initialize(); // producer-only

await audit.sendMessage('order.shipped', { orderId });
await orders.close();
```

Need the raw amqplib objects? `handler.connection`, `handler.channel`, and `import { amqp } from 'rabbit-node-handler'`.

## ⚙️ Configuration

`initializeRabbit(config)` and `new RabbitHandler(config)` accept:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `string` | `amqp://localhost` | Broker URL, e.g. `amqp://user:pass@host:5672/vhost` |
| `topic` | `string` | **required** | Exchange name (the Kafka "topic") |
| `groupId` | `string` | — | Queue name (the Kafka "consumer group"). Omit for a producer-only handler |
| `onNewMessage` | `function` | — | `({ routingKey, event, headers, redelivered, message }) => …`. Resolve to ack, throw to nack |
| `routingKeys` | `string[]` | `['#']` | Binding patterns for the queue (`*` one word, `#` zero or more) |
| `prefetch` | `number` | `1` | Unacknowledged messages in flight per consumer |
| `requeueOnError` | `boolean` | `false` | Requeue when the handler throws. Default drops the message (or dead-letters it if `queueOptions.deadLetterExchange` is set) |
| `exchangeType` | `string` | `'topic'` | `topic`, `direct`, `fanout` or `headers` |
| `durable` | `boolean` | `true` | Durable exchange and queue; messages are published persistent |
| `queueOptions` | `object` | `{}` | Extra `assertQueue` options (dead-letter exchange, TTL, max length…) |
| `handleSignals` | `boolean` | `true` | Close the connection and exit on `SIGINT` / `SIGTERM` |
| `logLevel` | `string` | `'info'` | `debug`, `info`, `warn` or `error` |
| `reconnect` | `object` | see below | Backoff settings |
| `onReconnectFailed` | `function` | — | Called if reconnecting gives up (only with a finite `maxRetries`) |

`reconnect` defaults: `{ initialDelay: 1000, maxDelay: 30000, maxRetries: Infinity, initialMaxRetries: 5 }` — the first connection fails fast after 5 attempts (so a misconfigured URL surfaces at startup), while a connection that drops later is retried forever.

### `sendMessage(routingKey, event, options?)`

Publishes `event` as JSON (`contentType: application/json`, persistent) to the exchange and resolves once the broker confirms it. `options` are passed straight to amqplib's `publish`, e.g. `{ headers: { traceId }, expiration: '60000', priority: 5 }`.

### `closeRabbit()` / `handler.close()`

Closes the connection and cancels any pending reconnect.

## 🔁 Coming from kafka-node-handler?

| Kafka | RabbitMQ (this package) |
|-------|-------------------------|
| `brokers: ['host:9092']` | `url: 'amqp://host:5672'` |
| `topic` | `topic` → a durable **topic exchange** |
| `groupId` | `groupId` → a durable **queue** bound to the exchange |
| message `key` | `routingKey` — and you can filter on it with `routingKeys` |
| `initializeKafka(...)` | `initializeRabbit(...)` |
| `sendMessage(key, event)` | `sendMessage(routingKey, event)` — now resolves on broker confirm, rejects on failure |
| handler errors are logged | handler errors are logged **and the message is nacked** (dropped, dead-lettered or requeued) |

## 🐛 Error Handling

Every operation is logged with a timestamp and a `[rabbit]` prefix. `initialize()` rejects if the broker can't be reached after `reconnect.initialMaxRetries` attempts; `sendMessage()` waits for a reconnect in progress and rejects if the broker refuses the message or the connection was closed. Handler exceptions never crash the process: the message is nacked and the error logged.

## 🧪 Tests

```bash
docker run -d -p 5672:5672 rabbitmq:4
npm install
npm test            # RABBITMQ_URL=amqp://... to point elsewhere
```

The suite runs against a real broker: routing-key filters, fan-out between groups, competing consumers, nack/requeue behaviour, broker confirms and fail-fast startup.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.
