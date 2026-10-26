import os
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Prioritize MONGODB_URI for Atlas cloud integration
MONGO_URI = os.getenv("MONGODB_URI", os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
MONGO_DB = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DB", "air_quality_db"))
METRICS_COLLECTION = os.getenv("MONGO_COLLECTION", "aggregated_metrics")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

app = FastAPI(
    title="Air Quality IoT Monitoring API — Indian CPCB Analytics",
    description="Real-time IoT Air Quality Analytics backend serving Spark aggregations and alerts from MongoDB Atlas",
    version="2.0.0"
)

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to convert MongoDB BSON fields (like _id, datetime) to JSON serializable formats."""
    clean = {}
    for k, v in doc.items():
        if k == "_id":
            clean["id"] = str(v)
        elif isinstance(v, datetime):
            clean[k] = v.isoformat()
        elif isinstance(v, dict):
            clean[k] = serialize_doc(v)
        elif isinstance(v, list):
            clean[k] = [serialize_doc(item) if isinstance(item, dict) else item for item in v]
        else:
            clean[k] = v
    return clean

@app.get("/api/health")
def health_check():
    mongo_connected = False
    # Safely mask URI: never expose usernames, passwords, or complete connection strings
    is_atlas = "mongodb.net" in MONGO_URI or "mongodb+srv" in MONGO_URI
    masked_target = "MongoDB Atlas (Cloud)" if is_atlas else "MongoDB Local/Container"
    details = {"target": masked_target, "database": MONGO_DB}

    try:
        client = get_mongo_client()
        client.admin.command('ping')
        mongo_connected = True
        db = client[MONGO_DB]
        details["metrics_count"] = db[METRICS_COLLECTION].estimated_document_count()
        details["alerts_count"] = db[ALERTS_COLLECTION].estimated_document_count()
        client.close()
    except Exception as e:
        details["error"] = "Connection to database failed. Verify network access and credentials."

    return {
        "status": "healthy" if mongo_connected else "degraded",
        "mongo_connected": mongo_connected,
        "details": details,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/stations")
def get_stations(city: Optional[str] = None):
    """Returns list of distinct monitoring stations available in the database."""
    stations = []
    query = {}
    if city:
        query["city"] = city

    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        coll = db[METRICS_COLLECTION]
        distinct_stations = coll.distinct("station_name", query)
        stations = [s for s in distinct_stations if s]
        client.close()
    except Exception:
        pass

    if not stations:
        # Fallback stations for Pune, Mumbai, Delhi
        stations = [
            "Revenue Colony-Shivajinagar, Pune - IITM",
            "Hadapsar, Pune - IITM",
            "Karve Road, Pune - MPCB",
            "Katraj Dairy, Pune - MPCB",
            "Bandra, Mumbai - MPCB",
            "Chhatrapati Shivaji Intl. Airport (T2), Mumbai - MPCB",
            "Anand Vihar, Delhi - DPCC",
            "R K Puram, Delhi - DPCC"
        ]

    return {"stations": stations, "count": len(stations)}

@app.get("/api/metrics/latest")
def get_latest_metrics(city: Optional[str] = None, station: Optional[str] = None):
    """Returns the latest window aggregation for monitored Indian cities / stations."""
    cities = [city] if city else ["Pune", "Mumbai", "Delhi"]
    results = {}
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        coll = db[METRICS_COLLECTION]

        for c in cities:
            q = {"city": c}
            if station:
                q["station_name"] = station
            doc = coll.find_one(q, sort=[("window.end", -1)])
            if doc:
                results[c] = serialize_doc(doc)
        client.close()
    except Exception:
        pass

    # If MongoDB is empty or stream is initializing, provide realistic CPCB sample preview
    if not results:
        now = datetime.now(timezone.utc)
        for c in cities:
            is_delhi = (c == "Delhi")
            is_pune = (c == "Pune")
            pm25 = 215.4 if is_delhi else (98.2 if is_pune else 74.5)
            pm10 = 310.0 if is_delhi else (158.0 if is_pune else 115.0)
            results[c] = {
                "city": c,
                "station_name": "Revenue Colony-Shivajinagar, Pune - IITM" if is_pune else f"{c} Central Station",
                "window": {
                    "start": now.isoformat(),
                    "end": now.isoformat()
                },
                "avg_pm25": pm25,
                "avg_pm10": pm10,
                "avg_no2": 42.1 if is_pune else 32.5,
                "avg_so2": 14.2,
                "avg_co": 1.45,
                "max_pm25": pm25 + 15.0,
                "valid_count": 58,
                "total_count": 60,
                "valid_rate": 0.9667,
                "data_source": "CPCB historical dataset",
                "_is_simulation_preview": True
            }

    return {"data": results, "count": len(results)}

@app.get("/api/metrics/history")
def get_metrics_history(
    city: Optional[str] = None,
    station: Optional[str] = None,
    limit: int = Query(default=30, le=100)
):
    """Returns historical window aggregates, optionally filtered by city or station."""
    query = {}
    if city:
        query["city"] = city
    if station:
        query["station_name"] = station

    records = []
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        coll = db[METRICS_COLLECTION]
        cursor = coll.find(query).sort("window.end", -1).limit(limit)
        records = [serialize_doc(doc) for doc in cursor]
        records.reverse()  # chronological order
        client.close()
    except Exception:
        pass

    if not records:
        # Generate representative Indian city history
        cities_to_gen = [city] if city else ["Pune", "Mumbai", "Delhi"]
        base_time = int(time.time()) - (limit * 60)
        for i in range(limit):
            t = base_time + (i * 60)
            iso_t = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            for c in cities_to_gen:
                spike = (c == "Delhi" and limit - 10 <= i <= limit - 4)
                records.append({
                    "city": c,
                    "station_name": "Revenue Colony-Shivajinagar, Pune - IITM" if c == "Pune" else f"{c} Central Station",
                    "window": {"start": iso_t, "end": iso_t},
                    "avg_pm25": round(210.0 + (i % 5) * 4 if spike else (92.0 + (i % 7) * 3 if c == "Pune" else 65.0 + (i % 4) * 2), 2),
                    "avg_pm10": round(315.0 if spike else 145.0 + (i % 6) * 4, 2),
                    "avg_no2": round(35.0 + (i % 4) * 2.0, 2),
                    "avg_so2": round(12.0 + (i % 3) * 1.0, 2),
                    "avg_co": round(1.35 + (i % 2) * 0.3, 2),
                    "max_pm25": round(240.0 if spike else 115.0, 2),
                    "valid_count": 59,
                    "total_count": 60,
                    "valid_rate": 0.9833,
                    "data_source": "CPCB historical dataset",
                    "_is_simulation_preview": True
                })

    return {"data": records, "count": len(records)}

@app.get("/api/alerts")
def get_alerts(city: Optional[str] = None, limit: int = Query(default=20, le=100)):
    """Returns recent alerts triggered by Spark streaming."""
    alerts = []
    query = {}
    if city:
        query["city"] = city

    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        coll = db[ALERTS_COLLECTION]
        cursor = coll.find(query).sort("processing_time", -1).limit(limit)
        alerts = [serialize_doc(doc) for doc in cursor]
        client.close()
    except Exception:
        pass

    if not alerts:
        # Fallback realistic alerts preview
        now = datetime.now(timezone.utc).isoformat()
        alerts = [
            {
                "city": "Delhi",
                "station_name": "Anand Vihar, Delhi - DPCC",
                "severity": "CRITICAL",
                "alert_reason": "avg_pm25>=150 and max_pm25>=150 (Severe)",
                "avg_pm25": 218.4,
                "max_pm25": 248.9,
                "processing_time": now,
                "_is_simulation_preview": True
            },
            {
                "city": "Pune",
                "station_name": "Karve Road, Pune - MPCB",
                "severity": "WARNING",
                "alert_reason": "max_pm25>=100 (Poor Spike)",
                "avg_pm25": 94.2,
                "max_pm25": 118.5,
                "processing_time": now,
                "_is_simulation_preview": True
            }
        ]

    return {"data": alerts, "count": len(alerts)}

@app.get("/api/stats")
def get_system_stats():
    """Aggregated pipeline quality and health metrics."""
    stats = {
        "total_windows": 0,
        "total_events_estimated": 0,
        "valid_events_estimated": 0,
        "average_valid_rate": 0.98,
        "total_alerts": 0,
        "critical_alerts": 0,
        "warning_alerts": 0,
        "database_target": "MongoDB Atlas (Cloud)",
        "simulation_mode": True
    }
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        m_coll = db[METRICS_COLLECTION]
        a_coll = db[ALERTS_COLLECTION]

        total_windows = m_coll.estimated_document_count()
        if total_windows > 0:
            stats["simulation_mode"] = False
            stats["total_windows"] = total_windows
            stats["total_alerts"] = a_coll.estimated_document_count()
            stats["critical_alerts"] = a_coll.count_documents({"severity": "CRITICAL"})
            stats["warning_alerts"] = a_coll.count_documents({"severity": "WARNING"})

            # Aggregation for validation rate
            pipeline = [
                {"$sort": {"window.end": -1}},
                {"$limit": 50},
                {"$group": {
                    "_id": None,
                    "sum_valid": {"$sum": "$valid_count"},
                    "sum_total": {"$sum": "$total_count"},
                    "avg_rate": {"$avg": "$valid_rate"}
                }}
            ]
            agg = list(m_coll.aggregate(pipeline))
            if agg:
                stats["valid_events_estimated"] = agg[0].get("sum_valid", 0)
                stats["total_events_estimated"] = agg[0].get("sum_total", 0)
                stats["average_valid_rate"] = round(agg[0].get("avg_rate", 1.0), 4)

        client.close()
    except Exception:
        pass

    return stats
