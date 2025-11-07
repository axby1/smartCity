// services/data-processor/kafkaConsumer.js
import { Kafka } from "kafkajs";
import { Telemetry } from "./mongo.js";

export const startConsumer = async () => {
  const kafka = new Kafka({
    clientId: "data-processor",
    brokers: ["kafka:9092"], // inside Docker network; localhost if local testing
  });

  const consumer = kafka.consumer({ groupId: "telemetry-consumer-group" });
  await consumer.connect();
  await consumer.subscribe({ topic: "telemetry", fromBeginning: false });

  console.log("📥 Data Processor connected to Kafka, waiting for messages...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        console.log("🔹 Received:", data);
        await Telemetry.create(data);
      } catch (err) {
        console.error("⚠️ Error processing message:", err.message);
      }
    },
  });
};
