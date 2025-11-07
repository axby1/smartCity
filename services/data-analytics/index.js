// Folder: services/data-analytics/index.js
import express from "express";
import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json());

// MongoDB connection
const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017/smartcity";
mongoose.connect(MONGO_URI)
  .then(() => console.log("✅ Connected to MongoDB (Analytics)"))
  .catch(err => console.error("❌ MongoDB connection failed:", err));

// Define schema
const telemetrySchema = new mongoose.Schema({
  sensorId: String,
  timestamp: Date,
  temperature: Number,
  humidity: Number,
  pollution: Number
});

const Telemetry = mongoose.model("Telemetry", telemetrySchema);

// 📊 Route: get average stats
app.get("/api/average", async (req, res) => {
  try {
    const avg = await Telemetry.aggregate([
      {
        $group: {
          _id: null,
          avgTemp: { $avg: "$temperature" },
          avgHumidity: { $avg: "$humidity" },
          avgPollution: { $avg: "$pollution" }
        }
      }
    ]);
    res.json(avg[0] || {});
  } catch (err) {
    console.error(err);
    res.status(500).send("Error fetching averages");
  }
});

// 🕐 Route: recent telemetry
app.get("/api/recent", async (req, res) => {
  try {
    const data = await Telemetry.find().sort({ timestamp: -1 }).limit(10);
    res.json(data);
  } catch (err) {
    res.status(500).send("Error fetching telemetry data");
  }
});

const PORT = process.env.PORT || 5002;
app.listen(PORT, () => console.log(`🚀 Analytics API running on port ${PORT}`));
