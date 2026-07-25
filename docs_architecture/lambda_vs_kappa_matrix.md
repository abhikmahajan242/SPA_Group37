# Architecture Evaluation: Lambda vs. Kappa for UrbanPulse

**Final Choice:** Lambda Architecture
**Justification:** While Kappa simplifies operations by maintaining only a single streaming code base, MetroConnect's specific mandate dictates heavy dual-reporting. The city requires sub-2-minute alerts for life-safety events (AQI emergencies, gridlock) *and* massive weekly/monthly batch aggregations for elected councillors. Relying solely on a Kappa architecture would force historical batch reporting to be processed through a stream engine, risking bottlenecks and impacting the latency of real-time alerts. A Lambda architecture safely air-gaps the heavy batch workloads (Spark) from the mission-critical real-time streams (Flink).

| Evaluation Criteria | Lambda Architecture (UrbanPulse Application) | Kappa Architecture (UrbanPulse Application) |
| :--- | :--- | :--- |
| **Latency** | **Optimal for Alerts:** Sub-2-minute AQI and 90-sec signal adaptations run on an isolated speed layer (Flink), ensuring zero interference from heavy batch queries. | **Risk of Bottlenecks:** Real-time alerts share processing resources with historical data replays, potentially breaching the 2-minute latency SLA during heavy loads. |
| **Fault Tolerance** | **High:** If the stream processor fails, the batch layer eventually corrects any inaccurate data. | **Moderate:** Relies entirely on the stream processor's internal state management and Kafka's retention policies for recovery. |
| **Operational Complexity** | **High:** Requires maintaining two separate codebases (e.g., Flink for streams, Spark for batch). | **Low:** A single codebase and processing framework handles both real-time streams and historical reprocessing. |
| **Reprocessing Capability** | **Excellent:** Reprocessing 12,000 bus GPS records for monthly reports is handled efficiently by dedicated batch engines reading from immutable object storage (Parquet). | **Challenging:** Requires rewinding the Kafka offset and re-streaming massive amounts of historical sensor data, which is slow and resource-intensive. |
| **Cost** | **Higher:** Requires provisioning infrastructure for both a speed layer and a batch layer. | **Lower:** Requires provisioning infrastructure for only a single streaming cluster. |
| **Compliance with Government Mandate** | **Compliant:** Easily handles the dual requirement of operational response and heavy councillor reporting. | **Non-Compliant:** Struggles to efficiently generate the required weekly/monthly councillor batch reports without risking operational latency. |
