package com.example.vitals;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Same input written with APIs that survive into Flink 2.x; the checker should stay quiet here. */
public class ModernVitalsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("vitals")
                .setGroupId("vitals-modern")
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-vitals")
                .map(VitalReading::parse)
                .print();
        env.execute("modern-vitals");
    }
}
