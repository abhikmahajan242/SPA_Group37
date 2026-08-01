package com.urbanpulse.detectors;

import com.urbanpulse.model.AqiEvent;
import com.urbanpulse.model.IncidentAlert;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Detects AQI Emergency incidents: a single reading where {@code aqi > 300}
 */
public class AqiEmergencyDetector extends KeyedProcessFunction<String, AqiEvent, IncidentAlert> {

    /** AQI threshold above which an emergency alert is raised (assignment spec). */
    private static final int AQI_EMERGENCY_THRESHOLD = 300;

    @Override
    public void processElement(AqiEvent event, Context ctx, Collector<IncidentAlert> out) {
        Integer aqi = event.getAqi();

        // Gracefully skip null AQI readings (simulated 5% sensor failure per the assignment).
        // Do not raise a false emergency on missing data.
        if (aqi == null) {
            return;
        }

        if (aqi > AQI_EMERGENCY_THRESHOLD) {
            IncidentAlert alert = new IncidentAlert(
                    "AQI_EMERGENCY",
                    event.getSensorId(),
                    event.getZone(),
                    "AQI reading of " + aqi + " exceeds emergency threshold of "
                            + AQI_EMERGENCY_THRESHOLD + " for sensor " + event.getSensorId(),
                    event.getTimestamp(),
                    System.currentTimeMillis()
            );
            out.collect(alert);
        }
    }
}