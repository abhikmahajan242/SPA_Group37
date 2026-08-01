package com.urbanpulse.detectors;

import com.urbanpulse.model.IncidentAlert;
import com.urbanpulse.model.TrafficSignalEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Detects Traffic Gridlock incidents: a junction where average wait time exceeds
 * 180 seconds for 3 consecutive traffic signal cycles
 */
public class GridlockDetector extends KeyedProcessFunction<String, TrafficSignalEvent, IncidentAlert> {

    /** Average wait time (seconds) above which a cycle is considered "gridlocked". */
    private static final double GRIDLOCK_WAIT_THRESHOLD = 180.0;

    /** Number of consecutive breaching cycles required to trigger an alert. */
    private static final int CONSECUTIVE_CYCLES_REQUIRED = 3;

    /**
     * Keyed state: count of consecutive breaching cycles for the current junction.
     * Initialized to 0 via the descriptor's default value. Managed by Flink's
     * state backend (RocksDB / Heap), checkpointed for fault tolerance.
     */
    private transient ValueState<Integer> breachCounter;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "breachCounter",
                Types.INT,
                0
        );
        breachCounter = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TrafficSignalEvent event, Context ctx, Collector<IncidentAlert> out) throws Exception {
        int count = breachCounter.value();

        if (event.getAvgWaitSec() > GRIDLOCK_WAIT_THRESHOLD) {
            // Breaching cycle — increment the consecutive counter
            count++;
            breachCounter.update(count);

            if (count == CONSECUTIVE_CYCLES_REQUIRED) {
                // First time we've hit 3 consecutive breaches — emit the alert
                String description = String.format(
                        "Junction %s avg wait %.1fs exceeded %ds for %d consecutive cycles in zone %s",
                        event.getJunctionId(),
                        event.getAvgWaitSec(),
                        (int) GRIDLOCK_WAIT_THRESHOLD,
                        CONSECUTIVE_CYCLES_REQUIRED,
                        event.getZone()
                );
                IncidentAlert alert = new IncidentAlert(
                        "GRIDLOCK",
                        event.getJunctionId(),
                        event.getZone(),
                        description,
                        event.getTimestamp(),
                        System.currentTimeMillis()
                );
                out.collect(alert);
            }
            // Note: counter continues past 3 so it won't re-alert until reset by a non-breach
        } else {
            // Non-breaching cycle — gridlock has cleared, reset counter
            breachCounter.update(0);
        }
    }
}