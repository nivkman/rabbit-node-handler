import type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, MessageProperties, Options } from "amqplib";
import type { EventEmitter } from "node:events";

export { default as amqp } from "amqplib";

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface MessageContext<T = unknown> {
  /** The routing key the message was published with. */
  routingKey: string;
  /** The message body, parsed as JSON (or the raw string when it is not JSON). */
  event: T;
  /** AMQP headers sent with the message. */
  headers: Record<string, unknown>;
  /** True when the broker is redelivering the message (e.g. after a requeue). */
  redelivered: boolean;
  /** The raw amqplib message. */
  message: ConsumeMessage;
}

export interface ReconnectOptions {
  /** First reconnect delay in ms. Default: 1000 */
  initialDelay?: number;
  /** Maximum reconnect delay in ms. Default: 30000 */
  maxDelay?: number;
  /** Backoff factor. Default: amqplib's (2) */
  factor?: number;
  /** Jitter fraction. Default: amqplib's */
  jitter?: number;
  /** Reconnect attempts once connected before giving up. Default: Infinity */
  maxRetries?: number;
  /** Attempts for the very first connection before `initialize()` rejects. Default: 5 */
  initialMaxRetries?: number;
  /** Custom delay strategy, in ms per attempt (starting at 1). */
  calculateDelay?: (attempt: number) => number;
}

export interface RabbitConfig<T = unknown> {
  /** Broker URL, e.g. `amqp://user:pass@host:5672/vhost`. Default: `amqp://localhost` */
  url?: string;
  /** Exchange name (the Kafka "topic"). Required. */
  topic: string;
  /** Queue name (the Kafka "consumer group"). Required to consume; omit for a producer-only handler. */
  groupId?: string;
  /** Called for every message. Resolve to ack; throw/reject to nack. */
  onNewMessage?: (context: MessageContext<T>) => void | Promise<void>;
  /** Binding patterns for the queue. Default: `["#"]` (everything) */
  routingKeys?: string[];
  /** Unacknowledged messages in flight per consumer. Default: 1 */
  prefetch?: number;
  /** Requeue a message when `onNewMessage` throws. Default: false (message is dropped or dead-lettered) */
  requeueOnError?: boolean;
  /** Exchange type. Default: "topic" */
  exchangeType?: "topic" | "direct" | "fanout" | "headers";
  /** Durable exchange and queue. Default: true */
  durable?: boolean;
  /** Extra `assertQueue` options, e.g. `{ deadLetterExchange: "dlx" }`. */
  queueOptions?: Options.AssertQueue;
  /** Close the connection and exit on SIGINT/SIGTERM. Default: true */
  handleSignals?: boolean;
  /** Minimum log level. Default: "info" */
  logLevel?: LogLevel;
  /** Reconnect/backoff settings. */
  reconnect?: ReconnectOptions;
  /** Called when reconnecting has given up (only possible with a finite `maxRetries`). */
  onReconnectFailed?: (error: Error) => void;
}

export type PublishOptions = Options.Publish;

export class RabbitHandler<T = unknown> {
  constructor(config: RabbitConfig<T>);
  /** The recovering amqplib connection (emits `connect`, `disconnect`, `reconnect-scheduled`, …). */
  readonly connection: EventEmitter | null;
  /** The confirm channel used for publishing (replaced after every reconnect). */
  readonly channel: ConfirmChannel | null;
  /** The current underlying amqplib ChannelModel (replaced after every reconnect). */
  readonly rawConnection: ChannelModel | null;
  initialize(): Promise<this>;
  sendMessage(routingKey: string, event: unknown, options?: PublishOptions): Promise<void>;
  close(): Promise<void>;
}

/** Connects the process-wide handler. Same shape as kafka-node-handler's `initializeKafka`. */
export function initializeRabbit<T = unknown>(config: RabbitConfig<T>): Promise<RabbitHandler<T>>;
/** Publishes `event` as JSON with the process-wide handler; resolves once the broker confirms. */
export function sendMessage(routingKey: string, event: unknown, options?: PublishOptions): Promise<void>;
/** Closes the process-wide handler. */
export function closeRabbit(): Promise<void>;

export type { Channel, ChannelModel, ConfirmChannel, ConsumeMessage, MessageProperties };
