# Flink 2.2 upgrade check

**BLOCKED**: fix the errors below before upgrading. 21 errors, 5 warnings, 4 notes.

| Fact | Value |
| --- | --- |
| project | `examples/legacy-job` |
| flinkVersion | `1.19.1` |
| javaLevel | `11` |
| operatorFlinkVersion | `1.19` |
| scanned | `6 source files, 1 build files, 2 config files` |
| rules | `361 removed classes, 687 removed members, 101 config keys (Flink 2.0 release notes); connector catalogue from Maven Central, 2026-11-19` |

## Errors (21)

### No release for Flink 2.x (as of 2026-11-19)

| Where | What | Fix |
| --- | --- | --- |
| `pom.xml:37` | flink-java:1.19.1 | The DataSet API module is gone. Move batch code to flink-streaming-java (BATCH mode) or Table API. |
| `pom.xml:48` | flink-connector-jdbc:3.2.0-1.19 | Split in 4.x: use flink-connector-jdbc-core plus the database module (flink-connector-jdbc-postgres, -mysql, -oracle, ...). |

### DataSet API was removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/DailySummaryBatchJob.java:3` | org.apache.flink.api.java.DataSet |
| `src/main/java/com/example/vitals/DailySummaryBatchJob.java:4` | org.apache.flink.api.java.ExecutionEnvironment |

**Fix:** Rewrite batch jobs on the DataStream API in BATCH mode, or on Table API/SQL. ([docs](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/dataset_migration/))

### SourceFunction was removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/SyntheticVitalsSource.java:3` | org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction |

**Fix:** Use a FLIP-27 Source (org.apache.flink.api.connector.source.Source), e.g. KafkaSource or DataGeneratorSource. ([docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/))

### Removed in Flink 2.0: RuntimeContext.getIndexOfThisSubtask(...)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/SyntheticVitalsSource.java:14` | org.apache.flink.api.common.functions.RuntimeContext: int getIndexOfThisSubtask() |

**Fix:** Use getRuntimeContext().getTaskInfo() for subtask index, parallelism, attempt and task names, and getRuntimeContext().getJobInfo() for the job id/name. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### RestartStrategies API was removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:4` | org.apache.flink.api.common.restartstrategy.RestartStrategies |

**Fix:** Set restart-strategy.type and its options on a Configuration (RestartStrategyOptions). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Flink's Time class was removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:8` | org.apache.flink.api.common.time.Time |

**Fix:** Use java.time.Duration (e.g. Duration.ofSeconds(5)). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Old state backend classes were removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:12` | org.apache.flink.runtime.state.filesystem.FsStateBackend |

**Fix:** Use HashMapStateBackend or EmbeddedRocksDBStateBackend, or set state.backend.type via config. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Class is gone from the Flink 2.x release of flink-connector-kafka

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:17` | org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer |

**Fix:** Move to the connector's FLIP-27 Source / Sink V2 API (e.g. KafkaSource, KafkaSink). ([docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/))

### Removed in Flink 2.0: Configuration.setInteger(...)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:30` | org.apache.flink.configuration.Configuration: void setInteger(java.lang.String, int) |

**Fix:** Use the typed API: conf.set(SomeOptions.OPTION, value) / conf.get(SomeOptions.OPTION). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Removed in Flink 2.0: StreamExecutionEnvironment.setStateBackend(...)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:35` | org.apache.flink.streaming.api.environment.StreamExecutionEnvironment: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment setStateBackend(org.apache.flink.runtime.state.StateBackend) |

**Fix:** Set state.backend.type (hashmap or rocksdb) in the Configuration you pass to getExecutionEnvironment(conf). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Removed in Flink 2.0: CheckpointConfig.setExternalizedCheckpointCleanup(...)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:36` | org.apache.flink.streaming.api.environment.CheckpointConfig: void setExternalizedCheckpointCleanup(org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup) |

**Fix:** Use setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### CheckpointConfig.ExternalizedCheckpointCleanup was removed

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:37` | org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup |

**Fix:** Use org.apache.flink.configuration.ExternalizedCheckpointRetention with CheckpointConfig.setExternalizedCheckpointRetention(...). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Removed in Flink 2.0: StreamExecutionEnvironment.setRestartStrategy(...)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:38` | org.apache.flink.streaming.api.environment.StreamExecutionEnvironment: void setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration) |

**Fix:** Set restart-strategy.type and its options in the Configuration. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Removed in Flink 2.0: RichFunction.open(Configuration)

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:78` | open(Configuration) override |

**Fix:** Override open(OpenContext openContext) instead (org.apache.flink.api.common.functions.OpenContext). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### flink-conf.yaml is no longer read by Flink 2.x

| Where | What |
| --- | --- |
| `conf/flink-conf.yaml:1` | Flink 2.x only reads config.yaml (standard YAML). |

**Fix:** Convert it with bin/migrate-config-file.sh from a Flink 1.19+/1.20 distribution, then rename to config.yaml. ([docs](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/config/#migrate-from-flink-confyaml-to-configyaml))

### Config option was removed in Flink 2.0

| Where | What |
| --- | --- |
| `conf/flink-conf.yaml:4` | taskmanager.heap.size |
| `conf/flink-conf.yaml:5` | jobmanager.heap.size |
| `conf/flink-conf.yaml:7` | state.backend.async |
| `k8s/flinkdeployment.yaml:10` | taskmanager.network.memory.buffers-per-channel |

**Fix:** Delete it or use its replacement option (see the Flink 2.0 configuration docs). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

## Warnings (5)

### Flink 2.x defaults to Java 17

| Where | What |
| --- | --- |
| `pom.xml:16` | Build targets Java 11 |

**Fix:** Move to Java 17; Flink 2.x images and docs assume it. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Flink module must move to a 2.x version

| Where | What |
| --- | --- |
| `pom.xml:25` | flink-streaming-java:1.19.1 -> flink-streaming-java:2.2.1 |
| `pom.xml:31` | flink-clients:1.19.1 -> flink-clients:2.2.1 |

**Fix:** Set the Flink version to 2.2.1 for all Flink modules. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### Connector needs its Flink 2.x release

| Where | What |
| --- | --- |
| `pom.xml:43` | flink-connector-kafka:3.2.0-1.19 -> flink-connector-kafka:5.0.0-2.2 |

**Fix:** Use 5.0.0-2.2; its major version changed, so check its release notes for removed APIs. ([docs](https://flink.apache.org/downloads/#apache-flink-connectors))

### Config key set in code was removed in Flink 2.0

| Where | What |
| --- | --- |
| `src/main/java/com/example/vitals/VitalsAlertJob.java:31` | "state.backend.async" |

**Fix:** Delete it or use its replacement option (see the Flink 2.0 configuration docs). ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

## Notes (4)

### Listed as removed in the 2.0 notes, but 2.0 still recognises this key

| Where | What |
| --- | --- |
| `conf/flink-conf.yaml:6` | state.backend.type |
| `conf/flink-conf.yaml:9` | web.port |
| `k8s/flinkdeployment.yaml:11` | state.backend.type |

**Fix:** Check the Flink 2.x docs for this key; it may now be a fallback for a new name. ([docs](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/))

### FlinkDeployment runs Flink 1.19

| Where | What |
| --- | --- |
| `k8s/flinkdeployment.yaml:7` | spec.flinkVersion: v1_19 |

**Fix:** Change spec.flinkVersion and spec.image together when you upgrade; take a savepoint first. ([docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/job-management/))

