import mongoose from "mongoose";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// load .env relative to repo root
dotenv.config({ path: path.join(__dirname, "../../.env") });

// Accept either name
const mongoUri = process.env.MONGO_URI;

if (!mongoUri || typeof mongoUri !== "string") {
  console.error("❌ MONGO_URI (or MONGODB_URI) is not set. Add to .env or docker-compose service env.");
  process.exit(1);
}

console.log("ℹ️ Connecting to MongoDB:", mongoUri);

(async () => {
  try {
    await mongoose.connect(mongoUri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log("✅ Connected to MongoDB");

    // dynamically import and start the Kafka consumer now that DB is connected
    const { startConsumer } = await import("./kafkaConsumer.js");
    startConsumer().catch((err) => {
      console.error("❌ Kafka consumer failed:", err);
      process.exit(1);
    });

    // graceful shutdown
    const shutdown = async () => {
      console.log("ℹ️ Shutting down data-processor...");
      try {
        await mongoose.disconnect();
      } catch (e) {}
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    // ...existing code... (if any further startup logic is required)
  } catch (err) {
    console.error("❌ Error connecting to MongoDB:", err);
    process.exit(1);
  }
})();
