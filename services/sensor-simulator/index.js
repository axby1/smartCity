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
  // sample sensor pool (optional — keeps stable ids/zones)
  const sensors = [
    { id: "sensor-001", zone: "north", lat: 40.7128, lon: -74.0060 },
    { id: "sensor-002", zone: "south", lat: 40.7060, lon: -74.0086 },
    { id: "sensor-003", zone: "east",  lat: 40.7150, lon: -74.0000 },
    { id: "sensor-004", zone: "west",  lat: 40.7100, lon: -74.0150 }
  ];
  const s = sensors[Math.floor(Math.random() * sensors.length)];

  const temperature = parseFloat((20 + Math.random() * 15).toFixed(2));
  const humidity = parseFloat((30 + Math.random() * 50).toFixed(2));
  const trafficDensity = Math.floor(Math.random() * 100);
  const pollutionLevel = parseFloat((50 + Math.random() * 50).toFixed(2));
  const batteryLevel = Math.round(20 + Math.random() * 80); // 20-100%
  const signalStrength = Math.round(50 + Math.random() * 50); // 50-100 dBm-ish
  const co2Level = Math.round(350 + Math.random() * 800); // ppm
  const noiseLevel = parseFloat((30 + Math.random() * 70).toFixed(1)); // dB
  const lightLevel = Math.round(Math.random() * 1000); // lux
  const occupancy = Math.random() < 0.2 ? 0 : Math.floor(Math.random() * 10); // 0..9 with some zeros
  const deviceModels = ["TX-100","X1-Pro","EnviroSense-v2"];
  const firmwareVersion = `v${Math.floor(1 + Math.random()*3)}.${Math.floor(Math.random()*10)}`;

  return {
    timestamp: new Date().toISOString(),
    sensor_id: s.id,
    zone: s.zone,
    latitude: parseFloat((s.lat + (Math.random()-0.5) * 0.001).toFixed(6)),
    longitude: parseFloat((s.lon + (Math.random()-0.5) * 0.001).toFixed(6)),
    deviceModel: deviceModels[Math.floor(Math.random() * deviceModels.length)],
    firmwareVersion,
    batteryLevel,
    status: batteryLevel < 25 ? "low_battery" : "ok",
    signalStrength,
    temperature,
    humidity,
    trafficDensity,
    pollutionLevel,
    co2Level,
    noiseLevel,
    lightLevel,
    occupancy
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
