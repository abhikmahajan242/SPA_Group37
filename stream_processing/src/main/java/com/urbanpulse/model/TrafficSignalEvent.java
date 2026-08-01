package com.urbanpulse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single traffic signal reading at a junction from the
 * {@code urbanpulse.traffic_signals} Kafka topic.
 * <p>
 * JSON schema:
 * <pre>{@code
 * {
 *   "junction_id": "JCT-01",
 *   "zone": "ZONE-A",
 *   "vehicle_count": 42,
 *   "avg_wait_sec": 210.5,
 *   "signal_phase": "RED",
 *   "timestamp": "2026-07-31T10:30:00Z"
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrafficSignalEvent {

    private String junctionId;
    private String zone;
    private int vehicleCount;
    private double avgWaitSec;
    private String signalPhase;
    private String timestamp;

    /**
     * Returns this event's timestamp as epoch milliseconds, used by Flink's
     * watermark / timestamp assigner for event-time processing.
     */
    public long getEventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }

}