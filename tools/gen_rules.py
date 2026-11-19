#!/usr/bin/env python3
"""Generate src/main/resources/rules/flink-core.json from the official Flink 2.0 release notes.

The release notes end with machine-generated lists of removed classes, removed
methods/fields/constructors and removed configuration options. This script turns
those lists into the rule file the scanner reads, so the rules stay traceable to
the upstream source instead of being typed by hand.

Usage:
    curl -sSfLo tools/flink-2.0.md \
      https://raw.githubusercontent.com/apache/flink/master/docs/content/release-notes/flink-2.0.md
    python3 tools/gen_rules.py tools/flink-2.0.md src/main/resources/rules/flink-core.json [jars-dir]

If a directory of Flink 2.0 jars is given, each removed method is checked against
the 2.0 class with javap. A method name that still exists in 2.0 (only one overload
was removed) is marked "overloadOnly", so the scanner does not flag every call to it.
"""
import glob
import json
import os
import re
import subprocess
import sys
from datetime import date

SOURCE_URL = "https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/"

# Package prefix -> group. First match wins, so keep more specific prefixes first.
GROUPS = [
    ("org.apache.flink.api.java.ExecutionEnvironment", "dataset"),
    ("org.apache.flink.api.java.DataSet", "dataset"),
    ("org.apache.flink.api.java.", "dataset"),
    ("org.apache.flink.streaming.api.functions.source.", "source-function"),
    ("org.apache.flink.streaming.api.functions.sink.", "sink-function"),
    ("org.apache.flink.api.connector.sink.", "sink-v1"),
    ("org.apache.flink.api.connector.sink2.", "sink-v1"),
    ("org.apache.flink.streaming.api.connector.sink2.", "sink-v1"),
    ("org.apache.flink.table.sources.", "table-source-sink"),
    ("org.apache.flink.table.sinks.", "table-source-sink"),
    ("org.apache.flink.table.descriptors.", "table-source-sink"),
    ("org.apache.flink.table.factories.", "table-source-sink"),
    ("org.apache.flink.table.api.TableSchema", "table-schema"),
    ("org.apache.flink.table.api.TableColumn", "table-schema"),
    ("org.apache.flink.table.api.Types", "table-schema"),
    ("org.apache.flink.table.api.constraints.", "table-schema"),
    ("org.apache.flink.api.common.restartstrategy.", "restart-strategy"),
    ("org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup", "checkpoint-retention"),
    ("org.apache.flink.api.common.time.Time", "time"),
    ("org.apache.flink.streaming.api.windowing.time.Time", "time"),
    ("org.apache.flink.runtime.state.memory.", "state-backend"),
    ("org.apache.flink.runtime.state.filesystem.", "state-backend"),
    ("org.apache.flink.contrib.streaming.state.", "state-backend"),
    ("org.apache.flink.state.api.", "state-processor"),
]

GROUP_INFO = {
    "dataset": {
        "title": "DataSet API was removed",
        "fix": "Rewrite batch jobs on the DataStream API in BATCH mode, or on Table API/SQL.",
        "link": "https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/dataset_migration/",
    },
    "scala": {
        "title": "Scala DataStream and DataSet APIs were removed",
        "fix": "Move the job to the Java DataStream API (it can still be called from Scala code).",
        "link": SOURCE_URL,
    },
    "source-function": {
        "title": "SourceFunction was removed",
        "fix": "Use a FLIP-27 Source (org.apache.flink.api.connector.source.Source), e.g. KafkaSource or DataGeneratorSource.",
        "link": "https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/",
    },
    "sink-function": {
        "title": "SinkFunction was removed",
        "fix": "Use a Sink V2 (org.apache.flink.api.connector.sink2.Sink), e.g. KafkaSink or FileSink.",
        "link": SOURCE_URL,
    },
    "sink-v1": {
        "title": "Sink V1 / old Sink V2 mix-ins were removed",
        "fix": "Implement the current Sink V2 interfaces (SupportsWriterState, SupportsCommitter).",
        "link": SOURCE_URL,
    },
    "table-source-sink": {
        "title": "Legacy TableSource/TableSink stack was removed",
        "fix": "Implement DynamicTableSource / DynamicTableSink with a DynamicTableFactory.",
        "link": "https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sourcessinks/",
    },
    "table-schema": {
        "title": "TableSchema, TableColumn and Types were removed",
        "fix": "Use Schema, Column and DataTypes.",
        "link": SOURCE_URL,
    },
    "restart-strategy": {
        "title": "RestartStrategies API was removed",
        "fix": "Set restart-strategy.type and its options on a Configuration (RestartStrategyOptions).",
        "link": SOURCE_URL,
    },
    "checkpoint-retention": {
        "title": "CheckpointConfig.ExternalizedCheckpointCleanup was removed",
        "fix": "Use org.apache.flink.configuration.ExternalizedCheckpointRetention with "
               "CheckpointConfig.setExternalizedCheckpointRetention(...).",
        "link": SOURCE_URL,
    },
    "time": {
        "title": "Flink's Time class was removed",
        "fix": "Use java.time.Duration (e.g. Duration.ofSeconds(5)).",
        "link": SOURCE_URL,
    },
    "state-backend": {
        "title": "Old state backend classes were removed",
        "fix": "Use HashMapStateBackend or EmbeddedRocksDBStateBackend, or set state.backend.type via config.",
        "link": SOURCE_URL,
    },
    "state-processor": {
        "title": "DataSet-based State Processor API classes were removed",
        "fix": "Use the DataStream-based SavepointReader / SavepointWriter.",
        "link": SOURCE_URL,
    },
    "other": {
        "title": "Class was removed in Flink 2.0",
        "fix": "See the Flink 2.0 release notes for the replacement.",
        "link": SOURCE_URL,
    },
    "member": {
        "title": "Method, field or constructor was removed in Flink 2.0",
        "fix": "See the Flink 2.0 release notes for the replacement; most config setters moved to Configuration/ConfigOption.",
        "link": SOURCE_URL,
    },
    "config": {
        "title": "Configuration option was removed in Flink 2.0",
        "fix": "Delete it or use its replacement option (see the Flink 2.0 configuration docs).",
        "link": SOURCE_URL,
    },
}

# Classes that were not dropped but moved to a public package in 2.0 (a one-line import fix).
# Only real public moves are listed: the release notes say the copies under "...legacy..."
# packages are internal and must not be used. Each target is checked against the 2.0 jars.
MOVED_CLASSES = {
    "org.apache.flink.api.java.utils.ParameterTool": "org.apache.flink.util.ParameterTool",
    "org.apache.flink.api.java.utils.MultipleParameterTool": "org.apache.flink.util.MultipleParameterTool",
    "org.apache.flink.api.java.utils.AbstractParameterTool": "org.apache.flink.util.AbstractParameterTool",
    "org.apache.flink.streaming.util.serialization.SimpleStringSchema":
        "org.apache.flink.api.common.serialization.SimpleStringSchema",
    "org.apache.flink.streaming.util.serialization.DeserializationSchema":
        "org.apache.flink.api.common.serialization.DeserializationSchema",
    "org.apache.flink.streaming.util.serialization.SerializationSchema":
        "org.apache.flink.api.common.serialization.SerializationSchema",
    "org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema":
        "org.apache.flink.api.common.serialization.AbstractDeserializationSchema",
    "org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema":
        "org.apache.flink.api.common.serialization.TypeInformationSerializationSchema",
    "org.apache.flink.runtime.state.CheckpointListener": "org.apache.flink.api.common.state.CheckpointListener",
    "org.apache.flink.streaming.api.functions.sink.DiscardingSink":
        "org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink",
    "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend$PriorityQueueStateType":
        "org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend$PriorityQueueStateType",
}


def classes_in_jars(jars_dir):
    import zipfile
    out = set()
    for j in glob.glob(os.path.join(jars_dir, "*.jar")):
        with zipfile.ZipFile(j) as z:
            for n in z.namelist():
                if n.endswith(".class") and n.startswith("org/apache/flink/"):
                    out.add(n[:-6].replace("/", "."))
    return out


# Hand-written replacement hints for the removed members people hit most often.
# Key: "<simple owner>#<member name>". Each replacement was checked against the 2.0 jars.
_TASK_INFO = ("Use getRuntimeContext().getTaskInfo() for subtask index, parallelism, attempt and task names, "
              "and getRuntimeContext().getJobInfo() for the job id/name.")
_CONF_TYPED = "Use the typed API: conf.set(SomeOptions.OPTION, value) / conf.get(SomeOptions.OPTION)."
_KRYO_REG = "Register types and serializers with the pipeline.serialization-config option instead."
MEMBER_HINTS = {
    **{f"RuntimeContext#{m}": _TASK_INFO for m in (
        "getIndexOfThisSubtask", "getNumberOfParallelSubtasks", "getMaxNumberOfParallelSubtasks",
        "getAttemptNumber", "getTaskName", "getTaskNameWithSubtasks", "getJobId")},
    "RuntimeContext#getExecutionConfig": "Use getRuntimeContext().createSerializer(...), getGlobalJobParameters() "
                                         "or isObjectReuseEnabled() instead of the ExecutionConfig.",
    **{f"Configuration#{m}": _CONF_TYPED for m in (
        "getInteger", "setInteger", "getLong", "setLong", "getBoolean", "setBoolean",
        "getFloat", "setFloat", "getDouble", "setDouble", "getClass", "setClass", "getBytes", "setBytes")},
    "StreamExecutionEnvironment#setStateBackend": "Set state.backend.type (hashmap or rocksdb) in the Configuration "
                                                  "you pass to getExecutionEnvironment(conf).",
    "StreamExecutionEnvironment#getStateBackend": "Read state.backend.type from the Configuration instead.",
    "StreamExecutionEnvironment#setRestartStrategy": "Set restart-strategy.type and its options in the Configuration.",
    "ExecutionConfig#setRestartStrategy": "Set restart-strategy.type and its options in the Configuration.",
    "CheckpointConfig#setExternalizedCheckpointCleanup":
        "Use setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION).",
    **{f"ExecutionConfig#{m}": _KRYO_REG for m in (
        "registerTypeWithKryoSerializer", "addDefaultKryoSerializer", "registerKryoType", "registerPojoType",
        "enableForceKryo", "disableForceKryo", "enableForceAvro", "disableForceAvro",
        "enableGenericTypes", "disableGenericTypes")},
    **{f"{o}#{m}": "Use window(TumblingEventTimeWindows.of(Duration.ofMinutes(1))) or the processing-time "
                   "equivalent." for o in ("KeyedStream", "DataStream") for m in ("timeWindow", "timeWindowAll")},
    "DataStream#writeAsText": "Use sinkTo(FileSink.forRowFormat(...).build()).",
    "DataStream#writeAsCsv": "Use sinkTo(FileSink.forRowFormat(...).build()).",
    "StreamExecutionEnvironment#readTextFile": "Use fromSource(FileSource.forRecordStreamFormat(new TextLineInputFormat(), path).build(), ...).",
}

# Whole packages that disappeared with their modules and are not in the class list.
REMOVED_PACKAGES = [
    {"prefix": "org.apache.flink.api.scala", "group": "scala"},
    {"prefix": "org.apache.flink.streaming.api.scala", "group": "scala"},
]


def group_for(fqcn: str) -> str:
    for prefix, group in GROUPS:
        if fqcn.startswith(prefix):
            return group
    return "other"


def section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    j = text.index(end, i)
    return text[i:j]


def parse_removed_classes(text: str):
    body = section(text, "### Removed Classes", "### Modified Classes")
    out = []
    for m in re.finditer(r"^- `([^`]+)`", body, re.M):
        fqcn = m.group(1)
        out.append({"fqcn": fqcn, "group": group_for(fqcn)})
    return out


METHOD_NAME = re.compile(r"([A-Za-z_$][\w$]*)\(")


def parse_removed_members(text: str):
    body = section(text, "### Modified Classes", "## List of removed configuration options")
    owner = None
    kind = None
    out = []
    for line in body.splitlines():
        m = re.match(r"^- `([^`]+)`", line)
        if m:
            owner, kind = m.group(1), None
            continue
        m = re.match(r"^    - (\w+) (removed|modified):", line)
        if m:
            kind = m.group(1) if m.group(2) == "removed" else None
            continue
        m = re.match(r"^        - `(.+)`\s*$", line)
        if m and owner and kind in ("method", "field", "constructor"):
            sig = m.group(1)
            if kind == "method":
                nm = METHOD_NAME.search(sig)
                name = nm.group(1) if nm else None
            elif kind == "field":
                name = sig.split()[-1]
            else:
                name = owner.rsplit(".", 1)[-1].split("$")[-1]
            if name:
                out.append({"owner": owner, "kind": kind, "name": name, "signature": sig})
    return out


def parse_removed_config(text: str):
    body = section(text, "## List of removed configuration options", "## List of REST APIs changes")
    keys = []
    for m in re.finditer(r"^- ([a-z0-9][\w.\-]*);?\s*$", body, re.M):
        keys.append(m.group(1))
    return keys


def surviving_method_names(owners, jars_dir):
    """Return {owner: set(method names)} for owners that still exist in the 2.0 jars."""
    jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
    cp = os.pathsep.join(jars)
    result = {}
    owners = sorted(owners)
    for i in range(0, len(owners), 40):
        batch = owners[i:i + 40]
        proc = subprocess.run(["javap", "-public", "-cp", cp, *batch], capture_output=True, text=True)
        current = None
        for line in proc.stdout.splitlines():
            m = re.match(r"^(?:public |protected )?(?:abstract |final |static |sealed |non-sealed )*(?:class|interface|enum|@interface|record) ([\w.$]+)", line)
            if m:
                current = m.group(1)
                result.setdefault(current, set())
                continue
            if current:
                nm = re.search(r"([A-Za-z_$][\w$]*)\(", line)
                if nm:
                    result[current].add(nm.group(1))
    return result


def config_keys_still_in_jars(keys, jars_dir):
    """Keys whose string still appears in 2.0 class files (kept as a fallback or still valid)."""
    import zipfile
    blob = bytearray()
    for j in glob.glob(os.path.join(jars_dir, "*.jar")):
        with zipfile.ZipFile(j) as z:
            for n in z.namelist():
                if n.endswith(".class"):
                    blob += z.read(n)
    return {k for k in keys if k.encode() in blob}


def main():
    src, dst = sys.argv[1], sys.argv[2]
    jars_dir = sys.argv[3] if len(sys.argv) > 3 else None
    text = open(src, encoding="utf-8").read()
    members = parse_removed_members(text)
    if jars_dir:
        owners = {m["owner"] for m in members if m["kind"] == "method"}
        alive = surviving_method_names(owners, jars_dir)
        for m in members:
            if m["kind"] != "method":
                continue
            if m["owner"] in alive:
                m["overloadOnly"] = m["name"] in alive[m["owner"]]
            else:
                # Owner class not found in 2.0 jars: it moved or was removed entirely.
                m["overloadOnly"] = False
    classes = parse_removed_classes(text)
    if jars_dir:
        present = classes_in_jars(jars_dir)
        missing = [t for t in MOVED_CLASSES.values() if t not in present]
        if missing:
            sys.exit(f"MOVED_CLASSES targets not in the 2.0 jars: {missing}")
    for c in classes:
        if c["fqcn"] in MOVED_CLASSES:
            c["movedTo"] = MOVED_CLASSES[c["fqcn"]]
    keys = parse_removed_config(text)
    still = config_keys_still_in_jars(keys, jars_dir) if jars_dir else set()
    config_rules = [{"key": k, "stillReferencedIn2x": k in still} for k in keys]
    rules = {
        "source": SOURCE_URL,
        "generatedOn": date.today().isoformat(),
        "groups": GROUP_INFO,
        "removedPackages": REMOVED_PACKAGES,
        "memberHints": MEMBER_HINTS,
        "removedClasses": classes,
        "removedMembers": members,
        "removedConfigKeys": config_rules,
    }
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=1)
        f.write("\n")
    print(
        f"{len(rules['removedClasses'])} classes, {len(rules['removedMembers'])} members, "
        f"{len(rules['removedConfigKeys'])} config keys -> {dst}"
    )


if __name__ == "__main__":
    main()
