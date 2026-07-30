```mermaid
graph TD
    %% Data Sources Layer
    subgraph Sources [Data Sources Layer]
        direction LR
        B["12,000 Buses"]
        T["3,800 Traffic Sensors"]
        A["600 AQI Monitors"]
        M["1.1M Smart Meters"]
    end

    %% Ingestion Layer
    subgraph Ingestion [Ingestion Layer]
        K["Apache Kafka 3-Broker Cluster"]
    end

    %% Processing Layer (Lambda)
    subgraph Processing [Processing Layer - Lambda]
        direction LR
        F["Apache Flink<br/>Real-Time Speed Layer"]
        S["Apache Spark<br/>15-Min Batch Layer"]
    end

    %% Storage Layer
    subgraph Storage [Storage Layer]
        direction LR
        P1[("PostGIS<br/>Geospatial Bus")]
        T1[("TimescaleDB<br/>Live Traffic")]
        P2[("PostgreSQL<br/>Councillor Aggregates")]
        M1[("MinIO Object Store<br/>Parquet Historical")]
    end

    %% Serving Layer
    subgraph Serving [Serving Layer]
        direction LR
        API["Real-Time ETA API"]
        SIG["Adaptive Signal Control Interface"]
        DASH["Apache Superset<br/>Ward Officer Dashboard"]
    end

    %% Flow Connections
    B -->|JSON| K
    T -->|JSON| K
    A -->|JSON| K
    M -->|JSON| K

    K -->|bus, traffic, aqi| F
    K -->|meters, aqi| S

    F --> P1
    F --> T1
    S --> P2
    S --> M1

    P1 --> API
    T1 --> SIG
    P2 --> DASH
    M1 --> DASH
