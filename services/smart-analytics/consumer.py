# Folder: services/smart-analytics/consumer.py
"""
Simple Kafka consumer using kafka-python running in a background thread.
Consumes telemetry messages, writes raw to MongoDB, updates ML engine, and optionally publishes analytics.
"""
import os
import json
import threading
import time
import traceback
from kafka import KafkaConsumer, KafkaProducer, errors
from dotenv import load_dotenv
from db import insert_raw, analytics_collection
from ml_engine import add_observation, predict_next

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CONSUME_TOPIC = os.getenv("KAFKA_CONSUME_TOPIC", "telemetry")
PUBLISH_TOPIC = os.getenv("KAFKA_PUBLISH_TOPIC", "analytics-output")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "smart-analytics-group")

producer = None
try:
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             retries=5)
except Exception as e:
    print("[consumer] Producer init failed:", e)

def _process_message(msg_value: dict):
    """
    Expected message format (flexible):
    { "sensor": "sensor-1", "zone": "zone-a", "timestamp": "...", "value": 12.3, ... }
    We'll try to find a numeric 'value' field, otherwise prefer 'temperature' or 'pollution'.
    """
    sensor_id = msg_value.get("sensor") or msg_value.get("sensorId") or msg_value.get("id") or msg_value.get("device")
    if not sensor_id:
        # fallback to zone-based id
        sensor_id = msg_value.get("zone", "unknown") + "-" + str(hash(json.dumps(msg_value)) % 10000)
    # find main numeric metric:
    value = None
    for key in ("value", "temperature", "pollution", "speed", "reading"):
        if key in msg_value:
            try:
                value = float(msg_value[key])
                break
            except Exception:
                continue
    # if still None, try to pick first numeric field
    if value is None:
        for k, v in msg_value.items():
            try:
                value = float(v)
                break
            except Exception:
                continue
    # store raw event in DB
    insert_raw({**msg_value, "sensor_id": sensor_id})
    # update ml state
    if value is not None:
        anom = add_observation(sensor_id, msg_value.get("timestamp"), value, raw_doc=msg_value)
        pred = predict_next(sensor_id)
        # store analytics doc
        analytics_doc = {
            "sensor_id": sensor_id,
            "timestamp": msg_value.get("timestamp") or time.time(),
            "value": value,
            "anomaly": anom,
            "prediction": pred
        }
        try:
            analytics_collection.insert_one(analytics_doc)
        except Exception as e:
            print("[consumer] analytics insert error:", e)
        # publish analytics to Kafka (optional)
        if producer:
            try:
                producer.send(PUBLISH_TOPIC, analytics_doc)
            except Exception as e:
                print("[consumer] publish analytics error:", e)

def create_consumer_with_retries(max_retries=0, base_delay=1.0):
    attempt = 0
    while True:
        try:
            attempt += 1
            print(f"[consumer] Attempting to create KafkaConsumer, broker: {KAFKA_BROKER}, attempt {attempt}")
            consumer = KafkaConsumer(
                CONSUME_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                group_id=GROUP_ID,
                enable_auto_commit=True,
                # do not set consumer_timeout_ms so iteration blocks
            )
            print("[consumer] KafkaConsumer created successfully")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"[consumer] No brokers available (attempt {attempt}).")
            if max_retries and attempt >= max_retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"[consumer] Retrying in {delay:.1f}s...")
            time.sleep(delay)
        except Exception:
            print("[consumer] Unexpected error creating consumer:")
            traceback.print_exc()
            time.sleep(base_delay)

def run_consumer_loop():
    consumer = create_consumer_with_retries(max_retries=0)  # 0 => infinite retries
    print(f"[consumer] Starting Kafka consumer, broker: {KAFKA_BROKER} topic: {CONSUME_TOPIC}")
    try:
        for msg in consumer:
            try:
                raw = msg.value
                # decode bytes -> str and parse JSON; tolerate already-parsed dicts
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        payload = json.loads(raw.decode("utf-8"))
                    except Exception as e:
                        print("[consumer] failed to json-decode message bytes:", e)
                        continue
                elif isinstance(raw, str):
                    try:
                        payload = json.loads(raw)
                    except Exception as e:
                        print("[consumer] failed to json-decode message string:", e)
                        continue
                elif isinstance(raw, dict):
                    payload = raw
                else:
                    print("[consumer] unsupported message type:", type(raw))
                    continue

                print("[consumer] message:", payload)
                _process_message(payload)
            except Exception as e:
                print("[consumer] Error processing message:", e)
    except Exception:
        print("[consumer] Consumer loop exited unexpectedly:")
        traceback.print_exc()
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        print("[consumer] consumer stopped")

def start_consumer():
    t = threading.Thread(target=run_consumer_loop, daemon=True)
    t.start()
    return t
