# UrbanPulse Streaming Platform 🏙️ 
**Domain:** Smart Cities & Urban Infrastructure

This repository contains the architecture, data ingestion, and stream processing pipelines for **UrbanPulse**, a real-time urban operations intelligence platform commissioned by the Smart Cities Mission for the fictitious tier-1 city of MetroConnect. 

## Repository Structure 
*Note: This documentation covers the foundational architecture and simulated telemetry streams. Downstream ingestion and processing pipelines will be documented in subsequent phases.*

```text
SPA_Group37/
├── data_generators/       # Python scripts generating simulated city telemetry
├── docs_architecture/     # Architecture diagrams and Task A matrices
├── kafka_ingestion/       # Kafka Cluster configs & Data Ingestion (Task B)
├── stream_processing/     # Flink & Spark Applications (Task C)
├── deliverables/          # Final project submission files
├── .gitignore             # Python and system junk file exclusion
└── README.md              # Project overview and setup instructions
