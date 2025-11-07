// Folder: services/sensor-simulator/index.js

import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// load .env reliably relative to this file
dotenv.config({ path: path.join(__dirname, "../../.env") });

// fallback and parsing
const rawBroker = process.env.KAFKA_BROKER || "localhost:9092";
const brokers = rawBroker.split(",").map(b => b.trim()).filter(Boolean);
const topic = (process.env.KAFKA_TOPIC || "telemetry").trim();

// validate topic
if (!topic || typeof topic !== "string" || !/^[A-Za-z0-9._-]+$/.test(topic)) {
  console.error("❌ Invalid or missing KAFKA_TOPIC:", JSON.stringify(process.env.KAFKA_TOPIC));
  process.exit(1);
}

console.log("ℹ️ Kafka brokers:", brokers);
console.log("ℹ️ Kafka topic:", topic);

const kafka = new Kafka({
  clientId: "sensor-simulator",
  brokers,
});

const producer = kafka.producer();

async function connectKafka() {
  try {
    await producer.connect();
    console.log("✅ Connected to Kafka Broker at", brokers.join(","));
  } catch (err) {
    console.error("❌ Failed to connect to Kafka:", err);
    process.exit(1);
  }
}

// Generate random telemetry data
function generateTelemetry() {
  return {
    timestamp: new Date().toISOString(),
    temperature: (20 + Math.random() * 10).toFixed(2),
    humidity: (40 + Math.random() * 30).toFixed(2),
    trafficDensity: Math.floor(Math.random() * 100),
    pollutionLevel: (50 + Math.random() * 50).toFixed(2),
  };
}

// Send data to Kafka every few seconds
async function startPublishing() {
  setInterval(async () => {
    const data = generateTelemetry();
    try {
      await producer.send({
        topic,
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
