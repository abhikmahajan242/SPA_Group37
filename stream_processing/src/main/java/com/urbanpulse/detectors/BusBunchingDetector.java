package com.urbanpulse.detectors;

import com.urbanpulse.model.BusGpsEvent;
import com.urbanpulse.model.IncidentAlert;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Detects Bus Bunching incidents: two buses on the same route within 200 meters
 * of each other for more than 5 consecutive minutes
 */
public class BusBunchingDetector extends KeyedProcessFunction<String, BusGpsEvent, IncidentAlert> {

    /** Maximum distance (meters) for two buses to be considered "bunched". */
    private static final double BUNCHING_DISTANCE_METERS = 200.0;

    /** Duration (milliseconds) that bunching must persist before an alert fires. */
    private static final long BUNCHING_DURATION_MS = 5 * 60 * 1000L;

    /**
     * MapState: busId → latest BusGpsEvent for every bus observed on this route.
     * Updated on every incoming event.
     */
    private transient MapState<String, BusGpsEvent> busPositions;

    /**
     * MapState: canonical pair key "busA:busB" → event-time timer timestamp.
     * Tracks which pairs are currently within 200m and being watched for the
     * 5-minute sustained duration.
     */
    private transient MapState<String, Long> bunchedPairs;

    @Override
    public void open(Configuration parameters) {
        busPositions = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("busPositions", Types.STRING, Types.POJO(BusGpsEvent.class))
        );
        bunchedPairs = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("bunchedPairs", Types.STRING, Types.LONG)
        );
    }

    @Override
    public void processElement(BusGpsEvent event, Context ctx, Collector<IncidentAlert> out) throws Exception {
        // Step 1: Update this bus's latest known position
        busPositions.put(event.getBusId(), event);

        // Step 2: Check every other bus on this route for proximity
        Long eventTime = ctx.timestamp();
        if (eventTime == null) {
            // No timestamp assigned (should not happen given the watermark assigner,
            // but guard defensively)
            return;
        }

        for (Map.Entry<String, BusGpsEvent> otherEntry : busPositions.entries()) {
            String otherBusId = otherEntry.getKey();
            if (otherBusId.equals(event.getBusId())) {
                continue;
            }

            BusGpsEvent other = otherEntry.getValue();
            double distance = haversineMeters(
                    event.getLat(), event.getLon(),
                    other.getLat(), other.getLon()
            );

            // Canonical pair key: sort bus IDs alphabetically so "BUS-001:BUS-002"
            // is the same regardless of which bus triggered this check
            String pairKey = event.getBusId().compareTo(otherBusId) < 0
                    ? event.getBusId() + ":" + otherBusId
                    : otherBusId + ":" + event.getBusId();

            if (distance < BUNCHING_DISTANCE_METERS) {
                // Buses are within range — start watching if not already
                if (!bunchedPairs.contains(pairKey)) {
                    long timerTimestamp = eventTime + BUNCHING_DURATION_MS;
                    ctx.timerService().registerEventTimeTimer(timerTimestamp);
                    bunchedPairs.put(pairKey, timerTimestamp);
                }
            } else {
                // Buses have separated beyond the threshold — stop watching
                bunchedPairs.remove(pairKey);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IncidentAlert> out) throws Exception {
        // Find all watched pairs whose timer matches this timestamp
        List<String> expiredPairs = new ArrayList<>();

        for (Map.Entry<String, Long> entry : bunchedPairs.entries()) {
            if (!entry.getValue().equals(timestamp)) {
                continue;
            }

            String pairKey = entry.getKey();
            String[] parts = pairKey.split(":", 2);
            String busA = parts[0];
            String busB = parts[1];

            BusGpsEvent a = busPositions.get(busA);
            BusGpsEvent b = busPositions.get(busB);

            if (a != null && b != null) {
                double distance = haversineMeters(
                        a.getLat(), a.getLon(),
                        b.getLat(), b.getLon()
                );

                if (distance < BUNCHING_DISTANCE_METERS) {
                    // Still bunched after 5 minutes — emit alert
                    String description = String.format(
                            "Buses %s and %s on route %s within %.0fm for over 5 minutes",
                            busA, busB, a.getRouteId(), distance
                    );
                    IncidentAlert alert = new IncidentAlert(
                            "BUS_BUNCHING",
                            pairKey,
                            "",
                            description,
                            a.getTimestamp(),
                            System.currentTimeMillis()
                    );
                    out.collect(alert);
                }
            }

            expiredPairs.add(pairKey);
        }

        // Clean up all expired pair entries
        for (String key : expiredPairs) {
            bunchedPairs.remove(key);
        }
    }

    private static final double EARTH_RADIUS_METERS = 6_371_000;

    /**
     * Calculates the great-circle distance between two geographic coordinates
     * using the Haversine formula.
     *
     * @return distance in meters
     */
    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_METERS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

}
