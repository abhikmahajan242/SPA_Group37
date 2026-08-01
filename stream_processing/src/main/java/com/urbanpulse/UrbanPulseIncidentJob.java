package com.urbanpulse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.urbanpulse.detectors.AqiEmergencyDetector;
import com.urbanpulse.detectors.BusBunchingDetector;
import com.urbanpulse.detectors.GridlockDetector;
import com.urbanpulse.model.AqiEvent;
import com.urbanpulse.model.BusGpsEvent;
import com.urbanpulse.model.IncidentAlert;
import com.urbanpulse.model.TrafficSignalEvent;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * UrbanPulse real-time incident detection pipeline entry point.
 * <p>
 * Wires up 3 Kafka sources (bus GPS, traffic signals, air quality), parses raw JSON
 * into typed POJOs, assigns event-time watermarks, and routes through 3 detectors:
 * <ol>
 *   <li>{@link AqiEmergencyDetector} — alerts when AQI > 300</li>
 *   <li>{@link GridlockDetector} — alerts when junction avg wait > 180s for 3 consecutive cycles</li>
 *   <li>{@link com.urbanpulse.detectors.BusBunchingDetector} — alerts when 2 buses on same route
 *       are within 200m for > 5 minutes</li>
 * </ol>
 * <p>
 * All alerts are serialized to JSON, unioned into a single stream, and sunk to
 * the {@code urbanpulse.incidents} Kafka topic.
 */
public class UrbanPulseIncidentJob {

    /*
     * Kafka bootstrap: use "kafka:29092" inside Docker (internal network),
     * or "localhost:9092" when running the job from an IDE on the host.
     */
    private static final String BOOTSTRAP_SERVERS = "kafka:29092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========================================================================
        // Source 1: Bus GPS (urbanpulse.bus_gps)
        // ========================================================================
        KafkaSource<String> busGpsSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("urbanpulse.bus_gps")
                .setGroupId("flink-incident-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<BusGpsEvent> busGpsStream = env
                .fromSource(busGpsSource, WatermarkStrategy.noWatermarks(), "bus_gps_raw")
                .map(new JsonMapper<>(BusGpsEvent.class))
                .returns(TypeInformation.of(BusGpsEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<BusGpsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTimeMillis())
                );

        // ========================================================================
        // Source 2: Traffic Signals (urbanpulse.traffic_signals)
        // ========================================================================
        KafkaSource<String> trafficSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("urbanpulse.traffic_signals")
                .setGroupId("flink-incident-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TrafficSignalEvent> trafficStream = env
                .fromSource(trafficSource, WatermarkStrategy.noWatermarks(), "traffic_signals_raw")
                .map(new JsonMapper<>(TrafficSignalEvent.class))
                .returns(TypeInformation.of(TrafficSignalEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficSignalEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTimeMillis())
                );

        // ========================================================================
        // Source 3: Air Quality (urbanpulse.air_quality)
        // ========================================================================
        KafkaSource<String> aqiSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("urbanpulse.air_quality")
                .setGroupId("flink-incident-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<AqiEvent> aqiStream = env
                .fromSource(aqiSource, WatermarkStrategy.noWatermarks(), "air_quality_raw")
                .map(new JsonMapper<>(AqiEvent.class))
                .returns(TypeInformation.of(AqiEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AqiEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTimeMillis())
                );

        // ========================================================================
        // Detector 1: AQI Emergency
        // ========================================================================
        DataStream<IncidentAlert> aqiAlerts = aqiStream
                .keyBy(AqiEvent::getSensorId)
                .process(new AqiEmergencyDetector());

        DataStream<String> aqiAlertsJson = aqiAlerts.map(new JsonSerializer<>());
        aqiAlertsJson.print("AQI_ALERT");

        // ========================================================================
        // Detector 2: Traffic Gridlock
        // ========================================================================
        DataStream<IncidentAlert> gridlockAlerts = trafficStream
                .keyBy(TrafficSignalEvent::getJunctionId)
                .process(new GridlockDetector());

        DataStream<String> gridlockAlertsJson = gridlockAlerts.map(new JsonSerializer<>());
        gridlockAlertsJson.print("GRIDLOCK_ALERT");

        // ========================================================================
        // Detector 3: Bus Bunching
        // ========================================================================
        DataStream<IncidentAlert> bunchingAlerts = busGpsStream
                .keyBy(BusGpsEvent::getRouteId)
                .process(new BusBunchingDetector());

        DataStream<String> bunchingAlertsJson = bunchingAlerts.map(new JsonSerializer<>());
        bunchingAlertsJson.print("BUNCHING_ALERT");

        // ========================================================================
        // Union all alert streams + sink to urbanpulse.incidents
        // ========================================================================
        DataStream<String> allAlerts = aqiAlertsJson.union(gridlockAlertsJson, bunchingAlertsJson);

        KafkaSink<String> incidentSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("urbanpulse.incidents")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        allAlerts.sinkTo(incidentSink);

        env.execute("UrbanPulse Incident Detection Job");
    }

    private static class JsonMapper<T> implements MapFunction<String, T> {
        private final Class<T> targetType;
        private transient ObjectMapper objectMapper;

        JsonMapper(Class<T> targetType) {
            this.targetType = targetType;
        }

        @Override
        public T map(String value) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            }
            return objectMapper.readValue(value, targetType);
        }
    }

    private static class JsonSerializer<T> implements MapFunction<T, String> {
        private transient ObjectMapper objectMapper;

        @Override
        public String map(T value) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.writeValueAsString(value);
        }
    }

}