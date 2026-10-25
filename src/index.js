import { RabbitHandler, initializeRabbit, sendMessage, closeRabbit } from "./rabbit-handler.js";

// Full access to amqplib, straight from this package.
export { default as amqp } from "amqplib";
export { RabbitHandler, initializeRabbit, sendMessage, closeRabbit };
