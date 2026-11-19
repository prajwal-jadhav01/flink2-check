package com.example.vitals;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Reads heart-rate readings, keeps the last 10 per patient and raises an alert when the
 * average goes above 120 bpm. Written the Flink 1.x way on purpose.
 */
public class VitalsAlertJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("taskmanager.numberOfTaskSlots", 2);
        conf.setString("state.backend.async", "true");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(60_000);
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.getConfig().setAutoWatermarkInterval(200);

        Properties kafka = new Properties();
        kafka.setProperty("bootstrap.servers", "localhost:9092");
        kafka.setProperty("group.id", "vitals-alerts");

        DataStream<VitalReading> readings = args.length > 0 && args[0].equals("--synthetic")
                ? env.addSource(new SyntheticVitalsSource()).uid("synthetic-source")
                : env.addSource(new FlinkKafkaConsumer<>("vitals", new SimpleStringSchema(), kafka))
                        .uid("kafka-source")
                        .map(new ParseReading());

        DataStream<String> alerts = readings
                .keyBy(r -> r.patientId)
                .process(new HighHeartRateAlert())
                .uid("alert-function");

        alerts.addSink(JdbcSink.sink(
                "INSERT INTO alerts (message) VALUES (?)",
                (stmt, msg) -> stmt.setString(1, msg),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/vitals")
                        .withDriverName("org.postgresql.Driver")
                        .build()))
                .uid("alerts-sink");

        env.execute("vitals-alert-job");
    }

    static class ParseReading extends RichMapFunction<String, VitalReading> {
        @Override
        public VitalReading map(String value) {
            return VitalReading.parse(value);
        }
    }

    static class HighHeartRateAlert extends KeyedProcessFunction<String, VitalReading, String> {
        private transient ValueState<RecentReadings> recent;

        @Override
        public void open(Configuration parameters) {
            recent = getRuntimeContext().getState(new ValueStateDescriptor<>("recent-readings", RecentReadings.class));
        }

        @Override
        public void processElement(VitalReading r, Context ctx, Collector<String> out) throws Exception {
            RecentReadings state = recent.value();
            if (state == null) {
                state = new RecentReadings();
                state.patientId = r.patientId;
            }
            state.lastHeartRates.add(r.heartRate);
            if (state.lastHeartRates.size() > 10) {
                state.lastHeartRates.remove(0);
            }
            recent.update(state);
            double avg = state.lastHeartRates.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            if (avg > 120) {
                out.collect("patient " + r.patientId + " average heart rate " + Math.round(avg));
            }
        }
    }
}
