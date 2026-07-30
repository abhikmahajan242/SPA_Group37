```mermaid
graph TD
    %% Data Sources Layer
    subgraph Sources [1. Data Sources Layer]
        B["12,000 GPS Buses"]
        T["3,800 Traffic Sensors"]
        A["600 AQI Monitors"]
        M["1.1M Smart Meters"]
    end

    %% Ingestion Layer
    subgraph Ingestion [2. Ingestion Layer]
        K["Apache Kafka 3-Broker Cluster"]
    end

    %% Data flow into Ingestion
    B -->|JSON| K
    T -->|JSON| K
    A -->|JSON| K
    M -->|JSON| K

    %% Processing Layer
    subgraph Processing [3. Processing Layer - Lambda]
        F["Apache Flink\nReal-Time Speed Layer"]
        S["Apache Spark\n15-Min Batch Layer"]
    end

    K -->|bus, traffic, aqi| F
    K -->|meters, aqi| S

    %% Storage Layer
    subgraph Storage [4. Storage Layer]
        P1[("PostGIS\nGeospatial Bus")]
        T1[("TimescaleDB\nLive Traffic")]
        P2[("PostgreSQL\nCouncillor Aggregates")]
        M1[("MinIO Object Store\nParquet Historical")]
    end

    F --> P1
    F --> T1
    S --> P2
    S --> M1

    %% Serving Layer
    subgraph Serving [5. Serving Layer]
        API["Real-Time ETA API"]
        SIG["Adaptive Signal Control"]
        DASH["Apache Superset\nWard Officer Dashboard"]
    end

    P1 --> API
    T1 --> SIG
    P2 --> DASH
    M1 --> DASH
