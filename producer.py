import os
import sys
import json
import time
import random
import csv
from datetime import datetime, timezone
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    KafkaProducer = None
    NoBrokersAvailable = Exception

# --- Configuration from Environment ---
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'air_quality_events')
INTERVAL_SECONDS = float(os.getenv('PRODUCER_INTERVAL', '1.0'))
DATASET_PATH = os.getenv('DATASET_PATH', 'data/cpcb_indian_air_quality.csv')
FILTER_CITIES_RAW = os.getenv('FILTER_CITIES', 'Pune,Mumbai,Delhi')
FILTER_CITIES = [c.strip() for c in FILTER_CITIES_RAW.split(',') if c.strip()]
REPLAY_LOOP = os.getenv('REPLAY_LOOP', 'true').lower() in ('true', '1', 'yes')

# Optional test spike injection
ENABLE_TEST_SPIKES = os.getenv('ENABLE_TEST_SPIKES', 'false').lower() in ('true', '1', 'yes')
SPIKE_CITY = os.getenv('SPIKE_CITY', 'Pune')
SPIKE_DELAY = int(os.getenv('SPIKE_DELAY', '30'))
SPIKE_DURATION = int(os.getenv('SPIKE_DURATION', '60'))

RANDOM_SEED = os.getenv('RANDOM_SEED')
if RANDOM_SEED is not None:
    random.seed(int(RANDOM_SEED))

cities = ["Pune", "Mumbai", "Delhi"]

def on_send_error(excp):
    print(f"[producer] Delivery failed: {excp}", file=sys.stderr)

def get_sensor_data(city_name):
    """Fallback synthetic generator kept for backward compatibility and unit tests."""
    return {
        "station_id": f"synthetic_{city_name.lower()}",
        "station_name": f"{city_name} Central Station",
        "city": city_name,
        "state": "Maharashtra" if city_name in ["Pune", "Mumbai"] else "Delhi",
        "timestamp": int(time.time()),
        "source_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pm25": round(random.uniform(25.0, 85.0), 2),
        "pm10": round(random.uniform(50.0, 140.0), 2),
        "no2": round(random.uniform(15.0, 45.0), 2),
        "so2": round(random.uniform(5.0, 15.0), 2),
        "co": round(random.uniform(0.5, 2.5), 2),
        "data_source": "synthetic_generator",
        "is_synthetic": True,
        "scenario_id": "synthetic_test"
    }

def get_spike_data(city_name):
    """Controlled spike generator used for alert testing."""
    return {
        "station_id": f"spike_{city_name.lower()}",
        "station_name": f"{city_name} High Exposure Zone",
        "city": city_name,
        "state": "Maharashtra" if city_name in ["Pune", "Mumbai"] else "Delhi",
        "timestamp": int(time.time()),
        "source_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pm25": round(random.uniform(205.0, 245.0), 2),
        "pm10": round(random.uniform(310.0, 360.0), 2),
        "no2": round(random.uniform(30.0, 60.0), 2),
        "so2": round(random.uniform(10.0, 25.0), 2),
        "co": round(random.uniform(1.5, 4.0), 2),
        "data_source": "synthetic_spike_injection",
        "is_synthetic": True,
        "scenario_id": "test_spike_critical"
    }

def safe_float(val):
    """Safely converts string to float, returning None for missing or unparseable entries."""
    if val is None or val == "" or val.lower() in ("nan", "na", "null"):
        return None
    try:
        f = float(val)
        return None if f < 0 else round(f, 2)
    except ValueError:
        return None

def parse_cpcb_row(row):
    """Parses and normalizes a row from the curated CPCB dataset."""
    pm25 = safe_float(row.get('pm25'))
    pm10 = safe_float(row.get('pm10'))
    no2 = safe_float(row.get('no2'))
    so2 = safe_float(row.get('so2'))
    co = safe_float(row.get('co'))

    # Raw row timestamp handling
    try:
        ts = int(row.get('timestamp', int(time.time())))
    except (ValueError, TypeError):
        ts = int(time.time())

    return {
        "station_id": row.get('station_id', 'unknown_station'),
        "station_name": row.get('station_name', 'Unknown Station'),
        "city": row.get('city', 'Unknown City'),
        "state": row.get('state', 'Unknown State'),
        "timestamp": ts,
        "source_timestamp": row.get('source_timestamp', datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
        "pm25": pm25,
        "pm10": pm10,
        "no2": no2,
        "so2": so2,
        "co": co,
        "data_source": row.get('data_source', 'CPCB historical dataset'),
        "is_synthetic": False,
        "scenario_id": row.get('scenario_id', 'cpcb_historical_replay')
    }

def main():
    max_retries = 30
    retry_delay = 2
    producer = None

    print(f"[producer] Connecting to Kafka broker at {KAFKA_BOOTSTRAP} (topic: {TOPIC})...")
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks='all'
            )
            print(f"[producer] Connected to Kafka on attempt {attempt}")
            break
        except NoBrokersAvailable:
            print(f"[producer] Broker not ready yet (attempt {attempt}/{max_retries}), retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    if producer is None:
        print("[producer] CRITICAL: Failed to connect to Kafka. Exiting.", file=sys.stderr)
        sys.exit(1)

    has_dataset = os.path.exists(DATASET_PATH)
    print("=" * 60)
    print("  DASSS Indian CPCB Air Quality Real-Time Replay Producer")
    print(f"  Dataset: {DATASET_PATH} (Exists: {has_dataset})")
    print(f"  Filter Cities: {FILTER_CITIES}")
    print(f"  Replay Interval: {INTERVAL_SECONDS}s per event batch")
    print(f"  Test Spikes Enabled: {ENABLE_TEST_SPIKES}")
    print("=" * 60)

    published_count = 0
    skipped_count = 0
    failed_count = 0
    iteration = 0

    spike_active = False
    spike_start_time = int(time.time()) + SPIKE_DELAY if ENABLE_TEST_SPIKES else 9999999999

    try:
        while True:
            iteration += 1
            if has_dataset:
                with open(DATASET_PATH, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        city = row.get('city', '')
                        if FILTER_CITIES and city not in FILTER_CITIES:
                            skipped_count += 1
                            continue

                        current_time = int(time.time())
                        if ENABLE_TEST_SPIKES:
                            if not spike_active and current_time >= spike_start_time:
                                print(f"[producer] !!! STARTING {SPIKE_DURATION}s TEST SPIKE FOR {SPIKE_CITY} !!!")
                                spike_active = True
                            if spike_active and current_time >= spike_start_time + SPIKE_DURATION:
                                print(f"[producer] !!! ENDING TEST SPIKE FOR {SPIKE_CITY} !!!")
                                spike_active = False
                                spike_start_time = 9999999999

                        if spike_active and city == SPIKE_CITY:
                            event = get_spike_data(city)
                        else:
                            event = parse_cpcb_row(row)

                        try:
                            producer.send(
                                TOPIC,
                                value=event,
                                key=city.encode('utf-8')
                            ).add_errback(on_send_error)
                            published_count += 1
                        except Exception as e:
                            failed_count += 1
                            print(f"[producer] Failed to send record: {e}", file=sys.stderr)

                        if published_count % 100 == 0:
                            print(f"[producer] Progress: Published={published_count}, Skipped={skipped_count}, Failed={failed_count}")
                            producer.flush()

                        time.sleep(INTERVAL_SECONDS)

                if not REPLAY_LOOP:
                    print(f"[producer] Dataset replay completed (Total published: {published_count}). Exiting.")
                    break
                else:
                    print(f"[producer] End of dataset reached (Pass {iteration}). Restarting stream replay...")
            else:
                # Fallback to generator if dataset path not mounted
                for city in FILTER_CITIES:
                    event = get_sensor_data(city)
                    producer.send(TOPIC, value=event, key=city.encode('utf-8'))
                    published_count += 1
                producer.flush()
                time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n[producer] Stopping gracefully. Final stats: Published={published_count}, Skipped={skipped_count}, Failed={failed_count}")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()