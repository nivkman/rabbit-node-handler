import { after, describe, it as baseIt } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import amqp from "amqplib";
import { RabbitHandler, initializeRabbit, sendMessage, closeRabbit } from "../src/index.js";

const URL = process.env.RABBITMQ_URL ?? "amqp://localhost";
const it = (name, fn) => baseIt(name, { timeout: 30_000 }, fn);
const base = { url: URL, handleSignals: false, logLevel: "error" };
const unique = (prefix) => `${prefix}-${randomUUID().slice(0, 8)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Resolves when `count` messages were collected, or rejects after `ms`. */
function collector(count, ms = 10_000) {
  const received = [];
  let resolve, reject;
  const done = new Promise((res, rej) => ((resolve = res), (reject = rej)));
  done.catch(() => {}); // a timed-out collector nobody awaited must not crash the run
  const timer = setTimeout(() => reject(new Error(`got ${received.length}/${count} messages`)), ms);
  const onNewMessage = (ctx) => {
    received.push(ctx);
    if (received.length >= count) {
      clearTimeout(timer);
      resolve(received);
    }
  };
  return { received, done, onNewMessage };
}

const cleanup = { exchanges: new Set(), queues: new Set() };
const topic = (name) => (cleanup.exchanges.add(name), name);
const group = (name) => (cleanup.queues.add(name), name);

after(async () => {
  await closeRabbit();
  const connection = await amqp.connect(URL);
  const channel = await connection.createChannel();
  for (const queue of cleanup.queues) await channel.deleteQueue(queue);
  for (const exchange of cleanup.exchanges) await channel.deleteExchange(exchange);
  await connection.close();
});

describe("config validation", () => {
  it("requires topic, and groupId + onNewMessage together", () => {
    assert.throws(() => new RabbitHandler({}), /"topic".*required/);
    assert.throws(() => new RabbitHandler({ topic: "t", onNewMessage() {} }), /"groupId".*required/);
    assert.throws(() => new RabbitHandler({ topic: "t", groupId: "g" }), /"onNewMessage".*required/);
    assert.throws(() => new RabbitHandler({ topic: "t", groupId: "g", onNewMessage: 1 }), /must be a function/);
  });
});

describe("module API (kafka-node-handler shape)", () => {
  it("rejects sendMessage before initializeRabbit", async () => {
    await assert.rejects(sendMessage("k", {}), /not initialized/);
  });

  it("delivers JSON events with routingKey and headers", async () => {
    const { done, onNewMessage } = collector(1);
    await initializeRabbit({ ...base, topic: topic(unique("ex")), groupId: group(unique("q")), onNewMessage });
    await sendMessage("user.created", { id: 7, name: "Niv" }, { headers: { source: "test" } });
    const [ctx] = await done;
    assert.equal(ctx.routingKey, "user.created");
    assert.deepEqual(ctx.event, { id: 7, name: "Niv" });
    assert.equal(ctx.headers.source, "test");
    assert.equal(ctx.redelivered, false);
    assert.equal(ctx.message.properties.contentType, "application/json");
    assert.equal(ctx.message.properties.deliveryMode, 2);
  });

  it("rejects sendMessage after closeRabbit", async () => {
    await closeRabbit();
    await assert.rejects(sendMessage("k", {}), /not initialized/);
  });
});

describe("RabbitHandler", () => {
  it("filters by routingKeys", async () => {
    const ex = topic(unique("ex"));
    const { done, received, onNewMessage } = collector(1);
    const consumer = await new RabbitHandler({ ...base, topic: ex, groupId: group(unique("q")), routingKeys: ["user.*"], onNewMessage }).initialize();
    const producer = await new RabbitHandler({ ...base, topic: ex }).initialize();
    try {
      await producer.sendMessage("order.created", { skip: true });
      await producer.sendMessage("user.updated", { ok: true });
      await done;
      await sleep(200);
      assert.deepEqual(received.map((c) => c.routingKey), ["user.updated"]);
    } finally {
      await Promise.all([consumer.close(), producer.close()]);
    }
  });

  it("fans out to every groupId, like Kafka consumer groups", async () => {
    const ex = topic(unique("ex"));
    const a = collector(1);
    const b = collector(1);
    const handlers = await Promise.all([
      new RabbitHandler({ ...base, topic: ex, groupId: group(unique("qa")), onNewMessage: a.onNewMessage }).initialize(),
      new RabbitHandler({ ...base, topic: ex, groupId: group(unique("qb")), onNewMessage: b.onNewMessage }).initialize(),
    ]);
    try {
      await handlers[0].sendMessage("ping", { n: 1 });
      const [[fromA], [fromB]] = await Promise.all([a.done, b.done]);
      assert.deepEqual(fromA.event, { n: 1 });
      assert.deepEqual(fromB.event, { n: 1 });
    } finally {
      await Promise.all(handlers.map((h) => h.close()));
    }
  });

  it("shares a groupId between competing consumers — each message delivered once", async () => {
    const ex = topic(unique("ex"));
    const q = group(unique("q"));
    const seen = [];
    let resolve;
    const all = new Promise((res) => (resolve = res));
    const onNewMessage = async ({ event }) => {
      seen.push(event.n);
      if (seen.length === 10) resolve();
    };
    const handlers = await Promise.all([
      new RabbitHandler({ ...base, topic: ex, groupId: q, onNewMessage }).initialize(),
      new RabbitHandler({ ...base, topic: ex, groupId: q, onNewMessage }).initialize(),
    ]);
    try {
      for (let n = 0; n < 10; n++) await handlers[0].sendMessage("job", { n });
      await all;
      await sleep(200);
      assert.deepEqual([...seen].sort((x, y) => x - y), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    } finally {
      await Promise.all(handlers.map((h) => h.close()));
    }
  });

  it("nacks without requeue when the handler throws (default)", async () => {
    let calls = 0;
    const handler = await new RabbitHandler({
      ...base, topic: topic(unique("ex")), groupId: group(unique("q")),
      onNewMessage: () => { calls++; throw new Error("boom"); },
    }).initialize();
    try {
      await handler.sendMessage("k", { v: 1 });
      await sleep(500);
      assert.equal(calls, 1);
    } finally {
      await handler.close();
    }
  });

  it("redelivers when requeueOnError is true", async () => {
    const { done, onNewMessage } = collector(2);
    let first = true;
    const handler = await new RabbitHandler({
      ...base, topic: topic(unique("ex")), groupId: group(unique("q")), requeueOnError: true,
      onNewMessage: (ctx) => { onNewMessage(ctx); if (first) { first = false; throw new Error("retry me"); } },
    }).initialize();
    try {
      await handler.sendMessage("k", { v: 1 });
      const [attempt1, attempt2] = await done;
      assert.equal(attempt1.redelivered, false);
      assert.equal(attempt2.redelivered, true);
    } finally {
      await handler.close();
    }
  });

  it("passes non-JSON bodies through as strings", async () => {
    const ex = topic(unique("ex"));
    const { done, onNewMessage } = collector(1);
    const handler = await new RabbitHandler({ ...base, topic: ex, groupId: group(unique("q")), onNewMessage }).initialize();
    const connection = await amqp.connect(URL);
    try {
      const channel = await connection.createChannel();
      channel.publish(ex, "raw", Buffer.from("plain text"));
      const [ctx] = await done;
      assert.equal(ctx.event, "plain text");
    } finally {
      await connection.close();
      await handler.close();
    }
  });

  it("rejects sendMessage after close()", async () => {
    const handler = await new RabbitHandler({ ...base, topic: topic(unique("ex")) }).initialize();
    await handler.close();
    await assert.rejects(handler.sendMessage("k", {}), /closed/);
  });

  it("reconnects after the connection drops and keeps consuming", async () => {
    const { done, onNewMessage } = collector(1);
    const handler = await new RabbitHandler({
      ...base, topic: topic(unique("ex")), groupId: group(unique("q")), onNewMessage,
      reconnect: { initialDelay: 50, maxDelay: 200 },
    }).initialize();
    try {
      const before = handler.rawConnection;
      const reconnected = new Promise((res) => handler.connection.once("connect", res));
      before.connection.stream.emit("error", new Error("simulated network drop")); // what a dropped socket looks like to amqplib
      await reconnected;
      assert.notEqual(handler.rawConnection, before);
      await handler.sendMessage("after.reconnect", { ok: true });
      const [ctx] = await done;
      assert.equal(ctx.routingKey, "after.reconnect");
    } finally {
      await handler.close();
    }
  });

  it("fails fast when the broker is unreachable", async () => {
    const handler = new RabbitHandler({
      ...base, url: "amqp://127.0.0.1:1", topic: "unreachable",
      reconnect: { initialDelay: 10, maxDelay: 20, initialMaxRetries: 2 },
    });
    await assert.rejects(handler.initialize());
  });
});
