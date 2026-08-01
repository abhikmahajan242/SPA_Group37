package com.urbanpulse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single bus GPS ping from the {@code urbanpulse.bus_gps} Kafka topic.
 * <p>
 * JSON schema:
 * <pre>{@code
 * {
 *   "bus_id": "BUS-001",
 *   "route_id": "R-101",
 *   "lat": 12.9716,
 *   "lon": 77.5946,
 *   "speed_kmh": 42.5,
 *   "occupancy_pct": 73.0,
 *   "timestamp": "2026-07-31T10:30:00Z"
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BusGpsEvent {

    private String busId;
    private String routeId;
    private double lat;
    private double lon;
    private double speedKmh;
    private double occupancyPct;
    private String timestamp;

    /**
     * Returns this event's timestamp as epoch milliseconds, used by Flink's
     * watermark / timestamp assigner for event-time processing.
     */
    public long getEventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }

}