package com.urbanpulse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single air quality reading from the {@code urbanpulse.air_quality} Kafka topic.
 * <p>
 * JSON schema:
 * <pre>{@code
 * {
 *   "sensor_id": "SENSOR-AQ-01",
 *   "zone": "ZONE-A",
 *   "aqi": 425,
 *   "pm25": 180.5,
 *   "pm10": 250.2,
 *   "timestamp": "2026-07-31T10:30:00Z"
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AqiEvent {

    private String sensorId;
    private String zone;
    private Integer aqi;
    private Double pm25;
    private Double pm10;
    private String timestamp;

    /**
     * Returns this event's timestamp as epoch milliseconds, used by Flink's
     * watermark / timestamp assigner for event-time processing.
     */
    public long getEventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }

}