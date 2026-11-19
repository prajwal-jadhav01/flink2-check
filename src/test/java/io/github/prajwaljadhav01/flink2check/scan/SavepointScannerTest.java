package io.github.prajwaljadhav01.flink2check.scan;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a real Flink 1.20 job on a MiniCluster, takes savepoints, and checks that the scanner
 * flags exactly the Kryo-serialised states.
 */
class SavepointScannerTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    /** A POJO with a List field: Flink 1.x serialises the list with Kryo. */
    public static class Visit {
        public String patientId;
        public List<String> codes = new ArrayList<>();
    }

    /** A POJO with only simple fields: Flink-native PojoSerializer all the way down. */
    public static class Summary {
        public String patientId;
        public long visits;
        public double lastValue;
    }

    static class StatefulFunction extends KeyedProcessFunction<Long, Long, Long> {
        private transient ValueState<Long> count;
        private transient ValueState<Visit> visit;
        private transient ValueState<Summary> summary;
        private transient ValueState<Map<String, Integer>> genericMap;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
            visit = getRuntimeContext().getState(new ValueStateDescriptor<>("visit", Visit.class));
            summary = getRuntimeContext().getState(new ValueStateDescriptor<>("summary", Summary.class));
            genericMap = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "generic-map", TypeInformation.of(new TypeHint<Map<String, Integer>>() { })));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
            count.update(count.value() == null ? 1L : count.value() + 1);
            Visit v = visit.value() == null ? new Visit() : visit.value();
            v.patientId = "P" + ctx.getCurrentKey();
            v.codes.add("I10");
            if (v.codes.size() > 5) {
                v.codes.remove(0);
            }
            visit.update(v);
            Summary s = summary.value() == null ? new Summary() : summary.value();
            s.patientId = v.patientId;
            s.visits++;
            s.lastValue = value;
            summary.update(s);
            Map<String, Integer> m = genericMap.value() == null ? new HashMap<>() : genericMap.value();
            m.merge("seen", 1, Integer::sum);
            genericMap.update(m);
        }
    }

    /** Has operator (non-keyed) list state with a Flink-native type. */
    static class BufferingMap implements MapFunction<Long, Long>, CheckpointedFunction {
        private transient ListState<String> buffer;

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            buffer.update(List.of("x"));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            buffer = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("buffer", String.class));
        }
    }

    private Path takeSavepoint(Path dir, String backend, SavepointFormatType format) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend.type", backend);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.fromSequence(0, Long.MAX_VALUE).uid("numbers")
                .map(new BufferingMap()).uid("buffering-map")
                .keyBy(v -> v % 10)
                .process(new StatefulFunction()).uid("stateful")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>());
        JobClient client = env.executeAsync("savepoint-source-job");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (client.getJobStatus().get() != JobStatus.RUNNING && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        Thread.sleep(Duration.ofSeconds(2).toMillis()); // let some records reach the stateful operator
        String path = client.triggerSavepoint(dir.toUri().toString(), format).get(60, TimeUnit.SECONDS);
        client.cancel().get(60, TimeUnit.SECONDS);
        return Path.of(java.net.URI.create(path));
    }

    /** mvn test -Ddemo.savepoint.dir=/some/dir keeps a copy of the savepoint for trying the CLI. */
    private static void keepForDemo(Path savepoint) throws java.io.IOException {
        String target = System.getProperty("demo.savepoint.dir");
        if (target == null || target.isBlank()) {
            return;
        }
        Path out = Path.of(target);
        java.nio.file.Files.createDirectories(out);
        try (var files = java.nio.file.Files.list(savepoint)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                java.nio.file.Files.copy(f, out.resolve(f.getFileName()),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private void assertFindings(Report report, SavepointScanner.Summary summary) {
        List<Finding> kryo = report.findings().stream().filter(f -> f.ruleId().equals("kryo-state")).toList();
        assertThat(kryo).extracting(Finding::location)
                .anyMatch(l -> l.endsWith("state \"visit\""))
                .anyMatch(l -> l.endsWith("state \"generic-map\""))
                .noneMatch(l -> l.endsWith("state \"count\""))
                .noneMatch(l -> l.endsWith("state \"summary\""))
                .noneMatch(l -> l.endsWith("state \"buffer\""));
        assertThat(kryo).filteredOn(f -> f.location().endsWith("\"visit\""))
                .first().extracting(Finding::detail).asString().contains("codes");
        assertThat(summary.states()).isGreaterThanOrEqualTo(5);
        assertThat(summary.atRisk()).isEqualTo(2);
        assertThat(summary.unreadable()).isZero();
    }

    @Test
    void flagsKryoStateInCanonicalHashMapSavepoint(@TempDir Path tmp) throws Exception {
        Path sp = takeSavepoint(tmp, "hashmap", SavepointFormatType.CANONICAL);
        keepForDemo(sp);
        Report report = new Report("2.2");
        SavepointScanner.Summary summary = new SavepointScanner(report, List.of()).scan(sp);
        assertFindings(report, summary);
    }

    @Test
    void flagsKryoStateInNativeRocksDbSavepoint(@TempDir Path tmp) throws Exception {
        Path sp = takeSavepoint(tmp, "rocksdb", SavepointFormatType.NATIVE);
        Report report = new Report("2.2");
        SavepointScanner.Summary summary = new SavepointScanner(report, List.of()).scan(sp);
        assertFindings(report, summary);
    }

    @Test
    void namesOperatorsByUid(@TempDir Path tmp) throws Exception {
        Path sp = takeSavepoint(tmp, "hashmap", SavepointFormatType.CANONICAL);
        Report report = new Report("2.2");
        new SavepointScanner(report, List.of(), java.util.Set.of("stateful", "buffering-map")).scan(sp);
        assertThat(report.findings()).filteredOn(f -> f.ruleId().equals("kryo-state"))
                .allMatch(f -> f.location().startsWith("operator uid \"stateful\""));
    }

    @Test
    void acceptsTheMetadataFileItself(@TempDir Path tmp) throws Exception {
        Path sp = takeSavepoint(tmp, "hashmap", SavepointFormatType.CANONICAL);
        Report report = new Report("2.2");
        SavepointScanner.Summary summary = new SavepointScanner(report, List.of()).scan(sp.resolve("_metadata"));
        assertThat(summary.atRisk()).isEqualTo(2);
    }
}
