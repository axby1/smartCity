// Folder: services/sensor-simulator/index.js

import { Kafka } from "kafkajs";
import dotenv from "dotenv";
dotenv.config({ path: "../../.env" });

const kafka = new Kafka({
  clientId: "sensor-simulator",
  brokers: [process.env.KAFKA_BROKER],
});

const producer = kafka.producer();

async function connectKafka() {
  await producer.connect();
  console.log("✅ Connected to Kafka Broker at", process.env.KAFKA_BROKER);
}

// Generate random telemetry data
function generateTelemetry() {
  return {
    timestamp: new Date().toISOString(),
    temperature: (20 + Math.random() * 10).toFixed(2),
    humidity: (40 + Math.random() * 30).toFixed(2),
    trafficDensity: Math.floor(Math.random() * 100),
    pollutionLevel: (50 + Math.random() * 50).toFixed(2)
  };
}

// Send data to Kafka every few seconds
async function startPublishing() {
  setInterval(async () => {
    const data = generateTelemetry();
    try {
      await producer.send({
        topic: process.env.KAFKA_TOPIC,
        messages: [{ value: JSON.stringify(data) }],
      });
      console.log("📡 Published:", data);
    } catch (err) {
      console.error("❌ Error publishing message:", err);
    }
  }, 3000);
}

(async () => {
  await connectKafka();
  await startPublishing();
})();
