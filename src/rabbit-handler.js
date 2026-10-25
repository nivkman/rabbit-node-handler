import amqp from "amqplib";
import { Logger } from "logger-standard";

const SERVICE_NAME = "rabbit";
const DEFAULT_RECONNECT = { initialDelay: 1000, maxDelay: 30000, maxRetries: Infinity, initialMaxRetries: 5 };
const DEFAULTS = {
  url: "amqp://localhost",
  routingKeys: ["#"],
  prefetch: 1,
  requeueOnError: false,
  exchangeType: "topic",
  durable: true,
  queueOptions: {},
  handleSignals: true,
  logLevel: "info",
};

export class RabbitHandler {
  /** @param {import("./index.d.ts").RabbitConfig} config */
  constructor(config = {}) {
    validateConfig(config);
    this._config = { ...DEFAULTS, ...config, reconnect: { ...DEFAULT_RECONNECT, ...config.reconnect } };
    this._logger = new Logger({ service: SERVICE_NAME, showDate: true, logLevel: this._config.logLevel });
    this._connection = null;
    this._model = null;
    this._channel = null;
    this._consumerChannel = null;
    this._consumerTag = null;
    this._inFlight = new Set();
    this._closed = false;
    this._ready = deferred();
  }

  /** The underlying amqplib connection (a recovering connection) — your escape hatch. */
  get connection() {
    return this._connection;
  }

  /** The confirm channel used for publishing (replaced after every reconnect). */
  get channel() {
    return this._channel;
  }

  /** The current underlying amqplib ChannelModel (replaced after every reconnect). */
  get rawConnection() {
    return this._model;
  }

  /** Connects, declares the exchange (and the consumer queue, if configured) and starts consuming. */
  async initialize() {
    const { url, reconnect } = this._config;
    this._connection = await amqp.connect(url, { recovery: { ...reconnect, setup: (model) => this._setup(model) } });
    this._listen();
    if (this._config.handleSignals) this._handleSignals();
    await this._ready.promise;
    return this;
  }

  /** Publishes `event` as JSON to the exchange with `routingKey`; resolves once the broker confirms it. */
  async sendMessage(routingKey, event, options = {}) {
    if (this._closed) throw new Error("Connection is closed. Call initialize() again to reconnect.");
    await this._ready.promise;
    const content = Buffer.from(JSON.stringify(event));
    const props = { persistent: true, contentType: "application/json", ...options };
    await publish(this._channel, this._config.topic, routingKey, content, props);
    this._logger.info(`Message sent successfully: ${routingKey}`);
  }

  /** Stops consuming, waits for in-flight messages to be acked, then closes the connection. */
  async close() {
    if (this._closed) return;
    this._closed = true;
    this._ready.reject(new Error("Connection is closed."));
    await this._stopConsuming();
    await this._connection?.close();
    this._logger.info("Connection closed gracefully");
  }

  // ─── Internals ────────────────────────────────────────────────────────────

  async _setup(model) {
    const { topic, exchangeType, durable, groupId, onNewMessage } = this._config;
    this._model = model;
    this._channel = await model.createConfirmChannel();
    await this._channel.assertExchange(topic, exchangeType, { durable });
    if (groupId && onNewMessage) await this._consume(model);
    this._ready.resolve();
    this._logger.info(`Connected — exchange "${topic}"${groupId ? `, queue "${groupId}"` : ""} ready`);
  }

  async _consume(model) {
    const { topic, groupId, routingKeys, prefetch, durable, queueOptions } = this._config;
    const channel = await model.createChannel();
    await channel.prefetch(prefetch);
    const { queue } = await channel.assertQueue(groupId, { durable, ...queueOptions });
    for (const key of routingKeys) await channel.bindQueue(queue, topic, key);
    const { consumerTag } = await channel.consume(queue, (message) => this._handle(channel, message), { noAck: false });
    this._consumerChannel = channel;
    this._consumerTag = consumerTag;
  }

  _handle(channel, message) {
    if (!message) return this._logger.warn("Consumer was cancelled by the broker");
    const task = this._process(channel, message).finally(() => this._inFlight.delete(task));
    this._inFlight.add(task);
  }

  async _process(channel, message) {
    const { routingKey, redelivered } = message.fields;
    let failure = null;
    try {
      const event = parseContent(message.content);
      await this._config.onNewMessage({ routingKey, event, headers: message.properties.headers ?? {}, redelivered, message });
    } catch (error) {
      failure = error;
      this._logger.error(`Error processing message "${routingKey}": ${error.message}`);
    }
    this._settle(channel, message, failure);
  }

  /** Acks or nacks; a closing channel only logs a warning — the broker redelivers unacked messages anyway. */
  _settle(channel, message, failure) {
    try {
      if (failure) channel.nack(message, false, this._config.requeueOnError);
      else channel.ack(message);
    } catch (error) {
      this._logger.warn(`Could not ${failure ? "nack" : "ack"} message "${message.fields.routingKey}": ${error.message}`);
    }
  }

  async _stopConsuming() {
    if (this._consumerChannel && this._consumerTag) {
      await this._consumerChannel.cancel(this._consumerTag).catch((error) => this._logger.warn(`Could not cancel consumer: ${error.message}`));
    }
    if (this._inFlight.size) {
      this._logger.info(`Waiting for ${this._inFlight.size} in-flight message(s)…`);
      await Promise.allSettled([...this._inFlight]);
    }
  }

  _listen() {
    this._connection.on("disconnect", (error) => {
      if (this._closed) return;
      this._ready = deferred();
      this._logger.warn(`Disconnected: ${error?.message ?? "connection closed"}. Reconnecting…`);
    });
    this._connection.on("reconnect-scheduled", ({ attempt, delay }) => {
      this._logger.warn(`Reconnect attempt ${attempt} in ${delay}ms`);
    });
    this._connection.on("reconnect-failed", (error) => {
      this._logger.error(`Max reconnection attempts reached: ${error.message}`);
      this._ready.reject(error);
      this._config.onReconnectFailed?.(error);
    });
    // Without an "error" listener a dropped socket would throw and crash the process instead of reconnecting.
    this._connection.on("error", (error) => this._logger.error(`Connection error: ${error.message}`));
    this._connection.on("handler-error", (error) => this._logger.error(`Handler error: ${error.message}`));
  }

  _handleSignals() {
    const shutdown = async (signal) => {
      this._logger.info(`${signal} received — shutting down…`);
      try {
        await this.close();
      } finally {
        process.exit(0);
      }
    };
    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
  }
}

function validateConfig(config) {
  if (!config.topic) throw new Error('"topic" (the exchange name) is required');
  if (config.onNewMessage && !config.groupId) throw new Error('"groupId" (the queue name) is required when "onNewMessage" is set');
  if (config.groupId && !config.onNewMessage) throw new Error('"onNewMessage" is required when "groupId" is set');
  if (config.onNewMessage && typeof config.onNewMessage !== "function") throw new Error('"onNewMessage" must be a function');
}

function publish(channel, exchange, routingKey, content, options) {
  return new Promise((resolve, reject) => {
    channel.publish(exchange, routingKey, content, options, (error) => (error ? reject(error) : resolve()));
  });
}

function parseContent(content) {
  const text = content.toString("utf8");
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => ((resolve = res), (reject = rej)));
  promise.catch(() => {}); // a rejected deferred nobody awaited must not crash the process
  return { promise, resolve, reject };
}

// ─── Module-level API (one handler per process, like kafka-node-handler) ──────

let handler = null;

/** Connects a process-wide handler. Same shape as kafka-node-handler's initializeKafka. */
export async function initializeRabbit(config) {
  if (handler) await handler.close();
  handler = new RabbitHandler(config);
  return handler.initialize();
}

/** Publishes with the process-wide handler. */
export function sendMessage(routingKey, event, options) {
  if (!handler) return Promise.reject(new Error("Handler is not initialized. Call initializeRabbit() first."));
  return handler.sendMessage(routingKey, event, options);
}

/** Closes the process-wide handler. */
export async function closeRabbit() {
  if (!handler) return;
  const current = handler;
  handler = null;
  await current.close();
}
