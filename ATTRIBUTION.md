# Academic Attribution & Licensing Notice

## Provenance
This project is developed for an academic case study on Real-Time IoT Air Quality Streaming and Analytics.

- **Original Base Repository:** `https://github.com/sucharithag18/real-time-iot-air-quality-pipeline`
- **Original Author:** `sucharitha1812` (sucharitha1812@gmail.com)
- **Modifications & Extensions:**
  - Complete code audit, defect corrections, and pipeline refactoring
  - Fixed Spark Structured Streaming aggregation logic to exclude invalid readings from pollutant averages
  - Corrected alert reasoning and aligned severity tiers
  - Resolved micro-batch duplicate writes with watermarked append output mode
  - Moved Spark checkpoints to persistent storage volumes
  - Added randomized seed, delivery callbacks, and intentional anomaly injection in Kafka producer
  - Added production-grade React IoT analytics dashboard (AuraStream)
  - Added Grafana dashboard with provisioning configurations
  - Added FastAPI backend connecting to MongoDB
  - Optimized resource utilization for 8 GB RAM machines (single-node local Spark streaming, JVM memory constraints, WiredTiger cache limits)

## License
The enhancements and additions made in this repository are provided under the MIT License for academic and non-commercial educational evaluation.
