#!/usr/bin/env python3
"""Generate src/main/resources/rules/connectors.json from Maven Central.

For every Flink artifact in ARTIFACTS this records:
  * whether any release targets Flink 2.x,
  * the newest release for Flink 1.20 and for each Flink 2.x minor,
  * for connectors that have both, the public classes that exist in the newest
    1.20 jar but are gone from the oldest 2.x jar (found by diffing the jars).

Usage:
    python3 tools/gen_connectors.py src/main/resources/rules/connectors.json [--no-diff]
"""
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date

REPO = "https://repo1.maven.org/maven2/org/apache/flink"

# Externalised connectors and formats (version looks like 3.4.0-1.20) and the core
# modules that a 1.x job typically depends on (version looks like 1.20.1).
ARTIFACTS = [
    "flink-connector-kafka", "flink-sql-connector-kafka",
    "flink-connector-jdbc", "flink-connector-jdbc-core", "flink-connector-jdbc-postgres",
    "flink-connector-jdbc-mysql", "flink-connector-jdbc-oracle",
    "flink-connector-elasticsearch7", "flink-connector-elasticsearch8",
    "flink-sql-connector-elasticsearch7",
    "flink-connector-opensearch", "flink-connector-opensearch2",
    "flink-connector-pulsar", "flink-sql-connector-pulsar",
    "flink-connector-kinesis", "flink-connector-aws-kinesis-streams",
    "flink-connector-aws-kinesis-firehose", "flink-connector-dynamodb",
    "flink-connector-rabbitmq", "flink-connector-cassandra_2.12",
    "flink-connector-hbase-2.2", "flink-connector-mongodb",
    "flink-connector-gcp-pubsub", "flink-connector-hive_2.12",
    "flink-connector-prometheus", "flink-connector-http",
    "flink-connector-files", "flink-connector-base", "flink-connector-datagen",
    "flink-java", "flink-scala_2.12", "flink-streaming-scala_2.12",
    "flink-streaming-java", "flink-clients", "flink-core", "flink-runtime",
    "flink-table-api-java", "flink-table-api-java-bridge", "flink-table-api-scala_2.12",
    "flink-table-api-scala-bridge_2.12", "flink-table-planner_2.12", "flink-table-planner-loader",
    "flink-table-runtime", "flink-statebackend-rocksdb", "flink-state-processor-api",
    "flink-cep", "flink-avro", "flink-avro-confluent-registry", "flink-json", "flink-csv",
    "flink-parquet", "flink-orc", "flink-protobuf", "flink-hadoop-compatibility_2.12",
    "flink-gelly", "flink-python", "flink-test-utils", "flink-runtime-web",
    "flink-metrics-prometheus", "flink-metrics-dropwizard",
]


# Artifacts whose 2.x successor has a different name.
MOVED = {
    "flink-connector-jdbc": "Split in 4.x: use flink-connector-jdbc-core plus the database module "
                            "(flink-connector-jdbc-postgres, -mysql, -oracle, ...).",
    "flink-connector-kinesis": "Use flink-connector-aws-kinesis-streams (KinesisStreamsSource / KinesisStreamsSink).",
    "flink-java": "The DataSet API module is gone. Move batch code to flink-streaming-java (BATCH mode) or Table API.",
    "flink-scala_2.12": "The Scala API is gone. Use the Java API modules.",
    "flink-streaming-scala_2.12": "The Scala DataStream API is gone. Use flink-streaming-java.",
    "flink-hadoop-compatibility_2.12": "Not released for 2.x.",
}


def fetch(url: str) -> bytes:
    for attempt in range(6):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            time.sleep(2 + attempt * 3)  # Maven Central rate-limits with 429
        except urllib.error.URLError:
            time.sleep(2 + attempt * 3)
    raise RuntimeError(f"giving up on {url}")


def versions(artifact: str):
    try:
        xml = fetch(f"{REPO}/{artifact}/maven-metadata.xml").decode()
    except urllib.error.HTTPError:
        return []
    return re.findall(r"<version>([^<]+)</version>", xml)


def flink_line(version: str):
    """Return the Flink minor a release targets, e.g. '1.20' or '2.1', or None."""
    m = re.search(r"-(\d+\.\d+)$", version)          # externalised: 3.4.0-1.20
    if m:
        return m.group(1)
    m = re.match(r"^(\d+\.\d+)\.\d+$", version)       # core: 1.20.1, 2.0.0
    if m:
        return m.group(1)
    return None


def vkey(v: str):
    return [int(x) if x.isdigit() else 0 for x in re.split(r"[.\-]", v)]


def public_classes(artifact: str, version: str):
    data = fetch(f"{REPO}/{artifact}/{version}/{artifact}-{version}.jar")
    names = set()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for n in z.namelist():
            if not n.endswith(".class") or "$" in n or "/internal/" in n:
                continue
            if not n.startswith("org/apache/flink/"):
                continue  # skip shaded third-party classes
            names.add(n[:-6].replace("/", "."))
    return names


def main():
    dst = sys.argv[1]
    diff = "--no-diff" not in sys.argv
    out = []
    for a in ARTIFACTS:
        vs = versions(a)
        time.sleep(0.5)
        if not vs:
            print(f"skip {a}: no metadata", file=sys.stderr)
            continue
        by_line = {}
        for v in vs:
            if "preview" in v or "SNAPSHOT" in v:
                continue
            line = flink_line(v)
            if line:
                by_line.setdefault(line, []).append(v)
        newest = {k: max(v, key=vkey) for k, v in by_line.items()}
        flink2 = sorted([k for k in newest if k.startswith("2.")], key=vkey)
        lines1 = sorted([k for k in newest if k.startswith("1.")], key=vkey)
        entry = {
            "artifactId": a,
            "latestFor1x": newest[lines1[-1]] if lines1 else None,
            "flink2Releases": {k: newest[k] for k in flink2},
        }
        if a in MOVED:
            entry["note"] = MOVED[a]
        if diff and entry["latestFor1x"] and flink2 and re.search(r"-\d+\.\d+$", entry["latestFor1x"]):
            first2 = min(by_line[flink2[0]], key=vkey)
            try:
                old = public_classes(a, entry["latestFor1x"])
                new = public_classes(a, first2)
                entry["removedClassesIn2x"] = sorted(old - new)
                entry["diffedAgainst"] = first2
            except Exception as e:  # noqa: BLE001 - a missing jar should not stop the run
                print(f"diff failed for {a}: {e}", file=sys.stderr)
        out.append(entry)
        print(f"{a}: 1.x={entry['latestFor1x']} 2.x={entry['flink2Releases']} "
              f"removed={len(entry.get('removedClassesIn2x', []))}")
    with open(dst, "w", encoding="utf-8") as f:
        json.dump({"source": "Maven Central", "generatedOn": date.today().isoformat(), "artifacts": out}, f, indent=1)
        f.write("\n")


if __name__ == "__main__":
    main()
