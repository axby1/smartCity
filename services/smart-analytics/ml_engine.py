# Folder: services/smart-analytics/ml_engine.py
"""
Simple ML engine:
- Maintains in-memory sliding windows per sensor (deque)
- Computes z-score anomaly on latest value using window stats
- Builds lag-feature dataset and trains a simple LinearRegression per sensor periodically
- Persists models to models/<sensor_id>.joblib
"""
import os
import time
import threading
from collections import deque, defaultdict
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

# config via env
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "50"))
MODEL_RETRAIN_INTERVAL = int(os.getenv("MODEL_RETRAIN_INTERVAL", "300"))
MODELS_DIR = Path("models")
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# in-memory storage: sensor_id -> deque of floats (recent primary metric)
windows = defaultdict(lambda: deque(maxlen=SLIDING_WINDOW_SIZE))
models = {}  # sensor_id -> sklearn model
last_trained = defaultdict(lambda: 0)

def add_observation(sensor_id: str, timestamp, value: float, raw_doc=None):
    """
    Add a new observation to the sliding window and return anomaly score (z-score).
    """
    dq = windows[sensor_id]
    dq.append(float(value))
    arr = np.array(dq)
    if len(arr) < 5:
        return {"z_score": None, "is_anomaly": False}
    mean = arr.mean()
    std = arr.std(ddof=0)
    if std == 0:
        z = 0.0
    else:
        z = (value - mean) / (std if std != 0 else 1.0)
    is_anom = abs(z) > 3.0  # simple threshold
    return {"z_score": float(z), "is_anomaly": bool(is_anom), "window_size": len(arr)}

def _build_lag_df(arr, lags=5):
    """Build lag features for a 1-D array"""
    df = pd.DataFrame({"y": arr})
    for i in range(1, lags + 1):
        df[f"lag_{i}"] = df["y"].shift(i)
    df = df.dropna()
    return df

def train_model_for_sensor(sensor_id: str, lags=5):
    """
    Train a simple linear regression model on lag features for that sensor from in-memory window.
    Persist model to disk.
    """
    arr = np.array(windows[sensor_id])
    if len(arr) < lags + 5:
        return False  # not enough data
    df = _build_lag_df(arr, lags=lags)
    X = df[[f"lag_{i}" for i in range(1, lags + 1)]].values
    y = df["y"].values
    model = LinearRegression()
    model.fit(X, y)
    models[sensor_id] = model
    joblib.dump(model, MODELS_DIR / f"{sensor_id}.joblib")
    last_trained[sensor_id] = time.time()
    return True

def predict_next(sensor_id: str, recent_values=None, lags=5):
    """
    Predict next value using persisted or in-memory model.
    recent_values: optional list/array of most recent values (length >= lags)
    """
    # try loaded model
    model = models.get(sensor_id)
    # load from disk if available
    model_path = MODELS_DIR / f"{sensor_id}.joblib"
    if model is None and model_path.exists():
        try:
            model = joblib.load(model_path)
            models[sensor_id] = model
        except Exception:
            model = None
    vals = None
    if recent_values is None:
        arr = np.array(windows[sensor_id])
        vals = arr[-lags:] if len(arr) >= lags else None
    else:
        vals = np.array(recent_values[-lags:]) if len(recent_values) >= lags else None
    if model is None or vals is None or len(vals) < lags:
        return None
    X = vals.reshape(1, -1)
    pred = model.predict(X)[0]
    return float(pred)

# background retrain thread
def periodic_retrain(interval=MODEL_RETRAIN_INTERVAL):
    while True:
        try:
            now = time.time()
            for sensor_id in list(windows.keys()):
                # retrain if enough data and last trained older than interval
                if now - last_trained[sensor_id] > interval:
                    trained = train_model_for_sensor(sensor_id)
                    if trained:
                        print(f"[ml_engine] Retrained model for {sensor_id}")
        except Exception as e:
            print("[ml_engine] retrain error:", e)
        time.sleep(interval)

# start background retrain thread on import
_thread = threading.Thread(target=periodic_retrain, args=(MODEL_RETRAIN_INTERVAL,), daemon=True)
_thread.start()
