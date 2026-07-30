```mermaid
graph TD
    %% Data Sources
    subgraph Sources [Data Sources Layer]
        B["12,000 Buses"] -->|JSON| K
        T["3,800 Traffic Sensors"] -->|JSON| K
        A["600 AQI Monitors"] -->|JSON| K
        M["1.1M Smart Meters"] -->|JSON| K
    end

    %% Ingestion
    subgraph Ingestion [Ingestion Layer]
        K(("Apache Kafka 3-Broker Cluster"))
    end

    %% Processing (Lambda Architecture)
    subgraph Processing [Processing Layer - Lambda]
        K -->|bus, traffic, aqi| F["Apache Flink\nReal-Time Speed Layer"]
        K -->|meters, aqi| S["Apache Spark\n15-Min Batch Layer"]
    end

    %% Storage
    subgraph Storage [Storage Layer]
        F --> P1[("PostGIS\nGeospatial Bus")]
        F --> T1[("TimescaleDB\nLive Traffic")]
        S --> M1[("MinIO Object Store\nParquet Historical")]
        S --> P2[("PostgreSQL\nCouncillor Aggregates")]
    end

    %% Serving
    subgraph Serving [Serving Layer]
        P1 --> API["Real-Time ETA API"]
        T1 --> SIG["Adaptive Signal Control Interface"]
        P2 --> DASH["Apache Superset\nWard Officer Dashboard"]
        M1 --> DASH
    end
