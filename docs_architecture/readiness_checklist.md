# UrbanPulse: Government Architecture Readiness Checklist

To meet the strict procurement and operational mandates of the Smart Cities Mission, the UrbanPulse deployment must satisfy the following 12 criteria across four critical domains.

### Data Sovereignty & Security
- [ ] **1. On-Premises Deployment:** The entire Kafka, Flink, and Spark cluster must be deployed on bare-metal servers physically located within MetroConnect municipal boundaries. No public cloud (AWS/GCP/Azure) compute may be utilized.
- [ ] **2. Air-Gapped Network Architecture:** The cluster must sit behind a strict municipal firewall, isolated from public internet access, to prevent external tampering with traffic signal control APIs.
- [ ] **3. At-Rest Encryption:** All time-series sensor data and historical Parquet files must be encrypted at rest using AES-256 to protect citizen transit and energy usage patterns.

### Open-Source Mandate
- [ ] **4. Zero Vendor Lock-in:** All core infrastructure components (Kafka, Flink, Spark, PostGIS, TimescaleDB) must be verified as 100% open-source under Apache 2.0, MIT, or similar permissible licenses.
- [ ] **5. Open Standards for Data Formats:** All storage must utilize open standards (e.g., JSON Lines for streaming, Parquet for batch, standard CSV for static joins) rather than proprietary vendor formats.
- [ ] **6. Community Support Verification:** All selected technologies must have an active open-source community with demonstrated long-term support to ensure the city is not stranded with deprecated software.

### Disaster Recovery (RPO < 15 min, RTO < 30 min)
- [ ] **7. Multi-Rack Partition Replication:** Kafka topic partitions must be replicated across at least three physically distinct server racks within the municipal data center to survive a localized hardware failure.
- [ ] **8. Automated State Checkpointing:** Apache Flink must be configured to take automated, incremental savepoints of its keyed state every 5 minutes to ensure an RPO of less than 15 minutes.
- [ ] **9. Active-Passive Database Failover:** The relational database serving the councillor reports must have a passive replica ready for automated promotion to active status within 10 minutes (satisfying the < 30 min RTO).

### Accessibility & Operations
- [ ] **10. Role-Based Access Control (RBAC):** Dashboards must restrict access so that ward officers can only view data and alerts specific to their assigned municipal ward.
- [ ] **11. No-Code Dashboard UI:** The serving layer (e.g., Apache Superset) must allow non-technical ward officers to visualize 15-minute energy aggregations and AQI advisories without writing SQL.
- [ ] **12. Centralized Dead-Letter Queue (DLQ) Logging:** All malformed sensor data (e.g., null AQI values, impossible GPS coordinates) must be automatically routed to a dedicated DLQ topic and visualized for IT investigation.
