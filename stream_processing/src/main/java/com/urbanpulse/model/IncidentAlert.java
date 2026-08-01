package com.urbanpulse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Common alert shape emitted by every detector and serialized to JSON
 * before being written to the {@code urbanpulse.incidents} Kafka topic.
 * <p>
 * JSON output example:
 * <pre>{@code
 * {
 *   "incidentType": "GRIDLOCK",
 *   "entityId": "JCT-01",
 *   "zone": "ZONE-A",
 *   "description": "Junction JCT-01 avg wait 210.5s exceeded 180s for 3 consecutive cycles",
 *   "sourceTimestamp": "2026-07-31T10:30:00Z",
 *   "detectedAtMillis": 1779265800123
 * }
 * }</pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IncidentAlert {

    private String incidentType;
    private String entityId;
    private String zone;
    private String description;
    private String sourceTimestamp;
    private long detectedAtMillis;

}