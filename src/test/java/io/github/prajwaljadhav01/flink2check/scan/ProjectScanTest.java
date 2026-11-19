package io.github.prajwaljadhav01.flink2check.scan;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import io.github.prajwaljadhav01.flink2check.rules.Rules;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Scans the sample Flink 1.19 job in examples/legacy-job. */
class ProjectScanTest {

    private static final Path SAMPLE = Path.of("examples/legacy-job").toAbsolutePath();
    private static Report report;

    @BeforeAll
    static void scan() {
        Rules rules = Rules.load();
        report = new Report("2.2");
        new SourceScanner(rules, report).scan(SAMPLE);
        new BuildScanner(rules, report, "2.2").scan(SAMPLE);
        new ConfigScanner(rules, report).scan(SAMPLE);
    }

    private static List<Finding> in(String fileSuffix) {
        return report.findings().stream().filter(f -> f.location().endsWith(fileSuffix)).toList();
    }

    private static boolean has(String fileSuffix, String detailPart) {
        return in(fileSuffix).stream().anyMatch(f -> f.detail().contains(detailPart));
    }

    @Test
    void findsRemovedClasses() {
        assertThat(has("VitalsAlertJob.java", "org.apache.flink.api.common.restartstrategy.RestartStrategies")).isTrue();
        assertThat(has("VitalsAlertJob.java", "org.apache.flink.api.common.time.Time")).isTrue();
        assertThat(has("VitalsAlertJob.java", "FsStateBackend")).isTrue();
        assertThat(has("VitalsAlertJob.java", "CheckpointConfig.ExternalizedCheckpointCleanup")).isTrue();
        assertThat(has("SyntheticVitalsSource.java", "RichParallelSourceFunction")).isTrue();
        assertThat(has("DailySummaryBatchJob.java", "org.apache.flink.api.java.ExecutionEnvironment")).isTrue();
    }

    @Test
    void findsConnectorClassesRemovedInTheirFlink2Release() {
        assertThat(in("VitalsAlertJob.java")).anyMatch(f ->
                f.ruleId().equals("removed-class/connector") && f.detail().endsWith("FlinkKafkaConsumer"));
    }

    @Test
    void findsRemovedMethodsWithResolvedReceivers() {
        assertThat(in("VitalsAlertJob.java")).filteredOn(f -> f.ruleId().equals("removed-method"))
                .extracting(Finding::title)
                .anyMatch(t -> t.contains("Configuration.setInteger"))
                .anyMatch(t -> t.contains("StreamExecutionEnvironment.setStateBackend"))
                .anyMatch(t -> t.contains("StreamExecutionEnvironment.setRestartStrategy"))
                .anyMatch(t -> t.contains("CheckpointConfig.setExternalizedCheckpointCleanup"))
                .anyMatch(t -> t.contains("RichFunction.open(Configuration)"));
        assertThat(in("SyntheticVitalsSource.java")).anyMatch(f ->
                f.title().contains("RuntimeContext.getIndexOfThisSubtask") && f.severity() == Severity.ERROR);
    }

    @Test
    void findsRemovedConfigKeysInCodeAndFiles() {
        assertThat(has("VitalsAlertJob.java", "\"state.backend.async\"")).isTrue();
        assertThat(in("flink-conf.yaml")).anyMatch(f -> f.ruleId().equals("legacy-flink-conf"));
        assertThat(in("flink-conf.yaml")).anyMatch(f -> f.detail().equals("taskmanager.heap.size")
                && f.severity() == Severity.ERROR);
        // still valid in 2.x, so only a note
        assertThat(in("flink-conf.yaml")).anyMatch(f -> f.detail().equals("state.backend.type")
                && f.severity() == Severity.INFO);
        assertThat(in("flinkdeployment.yaml")).anyMatch(f ->
                f.detail().equals("taskmanager.network.memory.buffers-per-channel"));
    }

    @Test
    void checksBuildFile() {
        List<Finding> pom = in("pom.xml");
        assertThat(pom).anyMatch(f -> f.ruleId().equals("no-flink2-release") && f.detail().startsWith("flink-java"));
        assertThat(pom).anyMatch(f -> f.ruleId().equals("no-flink2-release")
                && f.detail().startsWith("flink-connector-jdbc") && f.fix().contains("jdbc-core"));
        assertThat(pom).anyMatch(f -> f.ruleId().equals("bump-connector-version")
                && f.detail().contains("flink-connector-kafka:3.2.0-1.19 -> flink-connector-kafka:5.0.0-2.2"));
        assertThat(pom).anyMatch(f -> f.ruleId().equals("java-version") && f.severity() == Severity.WARNING);
        assertThat(report.facts()).containsEntry("flinkVersion", "1.19.1");
    }

    @Test
    void staysQuietOnCodeThatSurvivesFlink2() {
        assertThat(in("ModernVitalsJob.java")).isEmpty();
        assertThat(in("VitalReading.java")).isEmpty();
    }

    @Test
    void scansAGradleBuild(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        Files.writeString(tmp.resolve("build.gradle"), String.join("\n",
                "def flinkVersion = '1.18.1'",
                "java { sourceCompatibility = JavaVersion.VERSION_1_8 }",
                "dependencies {",
                "  implementation \"org.apache.flink:flink-streaming-java:${flinkVersion}\"",
                "  implementation 'org.apache.flink:flink-connector-rabbitmq:3.0.1-1.17'",
                "}"));
        Report r = new Report("2.2");
        new BuildScanner(Rules.load(), r, "2.2").scan(tmp);
        assertThat(r.findings()).anyMatch(f -> f.ruleId().equals("java-version") && f.severity() == Severity.ERROR);
        assertThat(r.findings()).anyMatch(f -> f.ruleId().equals("no-flink2-release")
                && f.detail().startsWith("flink-connector-rabbitmq"));
        assertThat(r.findings()).anyMatch(f -> f.ruleId().equals("bump-core-version")
                && f.detail().contains("flink-streaming-java:1.18.1"));
    }

    @Test
    void pointsMovedClassesToTheirNewPackage(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        Files.writeString(tmp.resolve("Args.java"), String.join("\n",
                "import org.apache.flink.api.java.utils.ParameterTool;",
                "class Args { ParameterTool p; }"));
        Report r = new Report("2.2");
        new SourceScanner(Rules.load(), r).scan(tmp);
        assertThat(r.findings()).singleElement().satisfies(f -> {
            assertThat(f.ruleId()).isEqualTo("moved-class");
            assertThat(f.fix()).contains("org.apache.flink.util.ParameterTool");
        });
    }

    @Test
    void scansScalaImports(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        Files.writeString(tmp.resolve("Job.scala"), String.join("\n",
                "import org.apache.flink.streaming.api.scala._",
                "object Job { def main(a: Array[String]): Unit = () }"));
        Report r = new Report("2.2");
        new SourceScanner(Rules.load(), r).scan(tmp);
        assertThat(r.findings()).anyMatch(f -> f.ruleId().equals("removed-package/scala"));
    }
}
