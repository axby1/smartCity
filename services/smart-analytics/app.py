# Folder: services/smart-analytics/app.py
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv
from consumer import start_consumer
from db import analytics_collection, raw_collection
from ml_engine import predict_next, windows
import threading
import time

load_dotenv()

app = FastAPI(title="Smart Analytics & Intelligence")

# start kafka consumer in background
_consumer_thread = start_consumer()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/predict/{sensor_id}")
def get_prediction(sensor_id: str):
    pred = predict_next(sensor_id)
    if pred is None:
        raise HTTPException(status_code=404, detail="Prediction not available for this sensor (insufficient data)")
    return {"sensor_id": sensor_id, "prediction": pred}

@app.get("/anomaly/{sensor_id}")
def get_anomaly(sensor_id: str):
    window = windows.get(sensor_id)
    if not window:
        raise HTTPException(status_code=404, detail="No data for sensor")
    # compute z score using ml_engine helper indirectly by examining last value
    # last value:
    last_val = window[-1] if len(window) > 0 else None
    from ml_engine import add_observation
    # call add_observation with same last_val to compute z_score (note: it also appends; avoid duplicating)
    # So compute inline instead:
    import numpy as np
    arr = np.array(window)
    mean = float(arr.mean()) if len(arr) else None
    std = float(arr.std(ddof=0)) if len(arr) else None
    z = None
    if last_val is not None and std and std != 0:
        z = (last_val - mean) / std
    return {"sensor_id": sensor_id, "last_value": last_val, "z_score": z, "window_size": len(window)}

@app.get("/insights/zone/{zone}")
def insights_zone(zone: str):
    # simple aggregation: average value per sensor in zone over last N docs
    docs = raw_collection.find({"zone": zone}).sort("timestamp", -1).limit(1000)
    records = list(docs)
    if not records:
        raise HTTPException(status_code=404, detail="No data for zone")
    # compute average for 'value' or 'temperature'
    vals = []
    for r in records:
        for key in ("value", "temperature", "pollution", "speed"):
            if key in r:
                try:
                    vals.append(float(r[key]))
                    break
                except Exception:
                    continue
    if not vals:
        raise HTTPException(status_code=404, detail="No numeric telemetry in zone")
    import statistics
    return {"zone": zone, "count": len(vals), "avg": statistics.mean(vals), "min": min(vals), "max": max(vals)}

@app.get("/insights/sensor/{sensor_id}")
def insights_sensor(sensor_id: str):
    docs = analytics_collection.find({"sensor_id": sensor_id}).sort("timestamp", -1).limit(500)
    recs = list(docs)
    if not recs:
        raise HTTPException(status_code=404, detail="No analytics for sensor")
    # compute anomaly rate in last records
    anoms = [1 for r in recs if r.get("anomaly", {}).get("is_anomaly")]
    return {"sensor_id": sensor_id, "records": len(recs), "anomaly_rate": len(anoms) / len(recs)}

# Basic websocket to stream analytics updates (optional)
@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await ws.accept()
    # naive: poll latest analytics every second and send
    try:
        while True:
            doc = analytics_collection.find().sort("timestamp", -1).limit(1)
            latest = None
            for d in doc:
                latest = d
            if latest:
                # remove ObjectId for JSON serializable
                latest.pop("_id", None)
                await ws.send_json(latest)
            await asyncio.sleep(1)
    except Exception:
        await ws.close()
