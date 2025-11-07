// services/data-processor/mongo.js
import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

export const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log("✅ Connected to MongoDB");
  } catch (err) {
    console.error("❌ MongoDB connection error:", err.message);
    process.exit(1);
  }
};

// Define a simple schema for telemetry data
const telemetrySchema = new mongoose.Schema({
  sensorId: String,
  temperature: Number,
  humidity: Number,
  airQuality: Number,
  timestamp: { type: Date, default: Date.now },
});

export const Telemetry = mongoose.model("Telemetry", telemetrySchema);
