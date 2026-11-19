# flink2-check

**Find out what breaks before you move a Flink 1.x job to Flink 2.x.**

Point it at a Flink job's repository, and optionally a savepoint. In a few seconds it lists every
removed API in the code, every connector with no 2.x release, every dead config key, and every
piece of saved state that Flink 2.x won't be able to restore. Each item gets a file and line number
and a suggested fix.

```
$ java -jar flink2-check.jar examples/legacy-job

ERROR (21)
  SourceFunction was removed
    src/main/java/com/example/vitals/SyntheticVitalsSource.java:3  org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
    fix: Use a FLIP-27 Source (org.apache.flink.api.connector.source.Source), e.g. KafkaSource or DataGeneratorSource.
  Class is gone from the Flink 2.x release of flink-connector-kafka
    src/main/java/com/example/vitals/VitalsAlertJob.java:17  org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
  Removed in Flink 2.0: RuntimeContext.getIndexOfThisSubtask(...)
    src/main/java/com/example/vitals/SyntheticVitalsSource.java:14  org.apache.flink.api.common.functions.RuntimeContext: int getIndexOfThisSubtask()
    fix: Use getRuntimeContext().getTaskInfo() for subtask index, parallelism, attempt and task names, ...
  Removed in Flink 2.0: RichFunction.open(Configuration)
    src/main/java/com/example/vitals/VitalsAlertJob.java:78  open(Configuration) override
    fix: Override open(OpenContext openContext) instead (org.apache.flink.api.common.functions.OpenContext).
  No release for Flink 2.x (as of 2026-11-19)
    pom.xml:48  flink-connector-jdbc:3.2.0-1.19
      fix: Split in 4.x: use flink-connector-jdbc-core plus the database module (flink-connector-jdbc-postgres, -mysql, -oracle, ...).
  ...
BLOCKED: fix the errors before moving to Flink 2.2 (21 errors, 5 warnings, 4 notes)

$ java -jar flink2-check.jar --savepoint ./savepoint-demo --job-jar target/test-classes --uid stateful

ERROR (2)
  State uses Kryo; Flink 2.x cannot restore it
    operator uid "stateful" / state "generic-map"  KEY_VALUE state "generic-map": value is Kryo-serialised (java.util.Map)
    operator uid "stateful" / state "visit"  KEY_VALUE state "visit": value.codes is Kryo-serialised (java.util.List)
    fix: Before upgrading, either rebuild this state from the source on Flink 2.x (start without the savepoint for this
         operator, or with a new uid), or migrate it on 1.20 to a Flink-native type (POJO without collection fields,
         Avro, Protobuf) with the State Processor API.
BLOCKED: fix the errors before moving to Flink 2.2 (2 errors, 0 warnings, 0 notes)
```

Both outputs above are real runs: the first on the bundled sample job, the second on a savepoint that
the test suite writes (`mvn test -Ddemo.savepoint.dir=./savepoint-demo`).

The full report for the bundled sample job is in [docs/sample-report.md](docs/sample-report.md).

## Why this exists

Flink 2.0 is the first major release in nine years. It removed the DataSet API, the Scala APIs,
`SourceFunction`/`SinkFunction`, the legacy `TableSource`/`TableSink` stack, `flink-conf.yaml`, and
hundreds of deprecated methods and config options
([release notes](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/)).
It also says plainly: *"State Compatibility is not guaranteed between 1.x and 2.x."* Kryo moved from
v2 to v5 and collections got new serializers, so state that used Kryo (generic types, POJOs with
`List`/`Map` fields, most Java records) fails to restore. You only find out after the upgrade,
when the job is stuck in a restart loop
([AWS state compatibility guide](https://docs.aws.amazon.com/managed-flink/latest/java/state-compatibility.html)).

The clock is running. Flink 1.19 lost security support on 1 July 2026, and Confluent's standard
support for 1.20, the last 1.x release, ended on 2 August 2026
([endoflife.date](https://endoflife.date/apache-flink)). Until now the advice was to read the
release notes, grep the logs, and test on a replica. `flink2-check` does the first two for you, so
you can use the replica test to confirm the fix, not to find the problems.

## What it checks

| Area | What it looks for | How |
| --- | --- | --- |
| **Code** (`*.java`, `*.scala`) | 268 classes removed from Flink core, plus classes each connector dropped in its 2.x release (e.g. `FlinkKafkaConsumer`); 280 removed methods and 379 removed fields; `open(Configuration)` overrides; removed config keys set from code; classes that only moved package (e.g. `ParameterTool` to `org.apache.flink.util`), with the new import | Parses the source with JavaParser and resolves names through imports and the declared types of variables. Calls on a known receiver type are errors. Calls where the receiver type is a guess are warnings. |
| **Build** (`pom.xml`, `build.gradle[.kts]`) | Flink modules and connectors with no 2.x release (`flink-java`, `flink-connector-jdbc`, …), the version to move to, renamed/split artifacts, third-party Flink libraries, Java level below 11 or 17 | Reads dependencies and resolves `${properties}`; looks each artifact up in a catalogue built from Maven Central |
| **Config** (`flink-conf.yaml`, `config.yaml`, Kubernetes `FlinkDeployment`) | Legacy `flink-conf.yaml` (no longer read by 2.x), 101 removed keys, `spec.flinkVersion` | YAML parsing, including the operator's `flinkConfiguration` block |
| **State** (savepoint or retained checkpoint) | Every keyed, operator and broadcast state whose serializer is Kryo or Scala, anywhere in the serializer tree (e.g. a `List` field inside a POJO), and state left by `FlinkKafkaConsumer`/`FlinkKafkaProducer` | Reads `_metadata` with Flink 1.20's own classes, opens only each operator's state **header** (never the data), and walks the serializer snapshots. Works for heap and RocksDB, canonical and native format. Operators are named by their `.uid("...")` from your code. |

## Quick start

Requirements: Java 17+.

```bash
git clone https://github.com/prajwal-jadhav01/flink2-check.git
cd flink2-check
mvn -B package                      # builds target/flink2-check.jar and runs the tests
java -jar target/flink2-check.jar examples/legacy-job
```

With Docker:

```bash
docker build -t flink2-check .
docker run --rm -v "$PWD/my-flink-job:/work" flink2-check /work
```

### Checking a savepoint

```bash
# 1. take a savepoint of the running 1.x job (or use a retained checkpoint)
flink savepoint <jobId> s3://my-bucket/savepoints
# 2. copy it to local disk (only _metadata and the small state files are read)
aws s3 cp --recursive s3://my-bucket/savepoints/savepoint-7f3a-... ./savepoint-7f3a
# 3. scan it, together with the code so operators get their uid names
java -jar flink2-check.jar ./my-flink-job --savepoint ./savepoint-7f3a --job-jar target/my-job.jar
```

`--job-jar` gives the scanner your job's classes (POJOs, Avro types) so it can read their serializer
snapshots. Without it, operators that store your own types are reported as unreadable rather than
silently skipped.

### Options

```
flink2-check [PROJECT] [--savepoint DIR] [--job-jar JAR]... [--uid UID]...
             [--target 2.0|2.1|2.2] [--format text|markdown|json] [--output FILE]
             [--fail-on error|warning|info|never] [--no-color]
```

Exit code is `1` when there is a finding at or above `--fail-on` (default `error`), so it can gate a
CI pipeline:

```yaml
# .github/workflows/flink2-readiness.yml
- run: curl -sSfLo flink2-check.jar https://github.com/prajwal-jadhav01/flink2-check/releases/latest/download/flink2-check.jar
- run: java -jar flink2-check.jar . --format markdown --output flink2.md --fail-on never
- run: cat flink2.md >> "$GITHUB_STEP_SUMMARY"
```

## Where the rules come from

None of the rules are typed in by hand from memory. They are generated, and you can re-run the
generators:

- [`tools/gen_rules.py`](tools/gen_rules.py) parses the machine-generated appendix of the official
  [Flink 2.0 release notes](tools/flink-2.0.md) (removed classes, removed methods/fields, removed
  config keys). With the Flink 2.0 jars it also checks each removed method name against the 2.0
  class, so a method that only lost one overload (`addSource`, `keyBy`) is not flagged everywhere.
  It also notes 28 "removed" keys that 2.0 still recognises, such as `state.backend.type`. Those are
  shown as notes, not errors. The Flink 2.1, 2.2 and 2.3 release notes add no further API removals.
- [`tools/gen_connectors.py`](tools/gen_connectors.py) reads Maven Central to find which connectors
  have a 2.x release. For each connector that has one, it diffs the newest 1.20 jar against the
  first 2.x jar to list the public classes that disappeared.
- A short list of replacement hints, such as `getRuntimeContext().getTaskInfo()` and
  `ExternalizedCheckpointRetention`, is hand-written. Each one was checked against the 2.0 jars.

## Tested against

- A sample Flink 1.19 job in [`examples/legacy-job`](examples/legacy-job), which compiles and runs
  on 1.19 and uses many of the removed APIs. It also has a `ModernVitalsJob` that must produce no
  findings.
- Real savepoints written by a Flink 1.20 MiniCluster during `mvn test`, using the heap backend in
  canonical format and RocksDB in native format. The tests assert that exactly the Kryo states are
  flagged and that Flink-native POJOs are not.
- Flink's own 1.17 example sources (streaming, batch, table). Every flagged line was checked against
  the source. Examples include `getRuntimeContext().getNumberOfParallelSubtasks()`,
  `CheckpointConfig.setCheckpointStorage(String)` and `SourceFunctionProvider`.

## Limits (read these)

- **Syntax, not compilation.** Names are resolved through imports and declared variable types, so a
  removed method called through a chain or a helper may be missed. Calls where the receiver is
  guessed are reported as warnings, not errors.
- **Java and Scala imports only.** PyFlink and SQL scripts are not scanned yet.
- **Savepoints must be on local disk.** Copy them from S3/HDFS/GCS first.
- **State check covers serializers, not semantics.** SQL/Table state is "compatible with caveats"
  per the Flink docs, so still test the restore on a copy before production.
- The connector catalogue is a snapshot (date shown in every report). Re-run the generator for the
  latest releases.

## Roadmap

- OpenRewrite recipes that rewrite the common cases automatically (`Time` to `Duration`,
  `getIndexOfThisSubtask()` to `getTaskInfo()`, `open(Configuration)` to `open(OpenContext)`).
- PyFlink and Flink SQL script scanning.
- Reading savepoints straight from S3.
- A GitHub Action wrapper.

Issues and pull requests are welcome.

## License

Apache License 2.0. See [LICENSE](LICENSE).
