# Design

## Problem

A team on Flink 1.x has to answer one question before it moves to 2.x: what breaks, and where? The
answer is spread over four places: the code (removed APIs), the build (connectors with no 2.x
release), the config (removed keys and the old `flink-conf.yaml`), and the saved state (Kryo state
that will not restore). Today that means reading a 1,600-line release note and running test
upgrades on replicas.

## Shape

```
          rules/flink-core.json  <- tools/gen_rules.py      <- Flink 2.0 release notes + 2.0 jars
          rules/connectors.json  <- tools/gen_connectors.py <- Maven Central metadata + jar diffs
                    |
                    v
 project dir --> SourceScanner  (JavaParser: imports, variable types, method calls, string literals)
             --> BuildScanner   (pom.xml / build.gradle: versions, missing 2.x releases, Java level)
             --> ConfigScanner  (flink-conf.yaml, config.yaml, FlinkDeployment)
 savepoint   --> SavepointScanner (Flink 1.20 classes: _metadata -> state headers -> serializer tree)
                    |
                    v
                 Report --> text | markdown | json, exit code for CI
```

## Decisions

1. **Rules are generated, not hand-written.** The release notes end with machine-generated lists of
   removed classes, members and config keys. Parsing them keeps every rule traceable to its source
   and makes the rule set easy to refresh. A small, hand-written list of replacement hints sits on
   top, and each one was checked against the 2.0 jars.
2. **Check removed methods against the 2.0 jars.** The release notes list removed *signatures*, so
   `addSource`, `keyBy` and `enableCheckpointing` all appear even though other overloads remain. The
   generator runs `javap` on the 2.0 classes and marks those names `overloadOnly`, and the scanner
   ignores them. Without this, most real jobs would get dozens of false positives.
3. **Some config keys listed as removed still exist.** 28 of the 101 keys, `state.backend.type`
   among them, still appear in the 2.0 class files (as the new key or as a fallback). They are
   reported as notes, not errors.
4. **Syntax-level resolution, two confidence levels.** Compiling the user's project would need
   their whole build. Instead, names are resolved through imports, wildcard imports and declared
   variable, field and parameter types, plus a few common getter chains such as `getRuntimeContext()`
   and `getCheckpointConfig()`. A call on a resolved receiver is an ERROR. When the receiver is
   unknown and the method name vanished entirely from a commonly used class, it is a WARNING.
   Generic names like `of`, `open` and `apply` are never guessed.
5. **Read state headers, not state.** Every keyed or operator state stream starts with a header
   that lists each state and the snapshot of the serializer that wrote it. Reading only that header
   with Flink 1.20's own `KeyedBackendSerializationProxy`/`OperatorBackendSerializationProxy` makes
   the check fast, even for very large RocksDB state. It also keeps the scanner away from the
   values, which may be PHI or PII.
6. **Walk the serializer tree by reflection.** Problems hide deep, e.g. a `List` field inside a
   POJO inside a `ValueState`. Snapshot classes keep their children in private fields, so the walker
   descends into Flink's own holder objects (and only those) and reports the path to the Kryo node
   (`value.codes`).
7. **Name operators by uid.** Savepoints only hold operator ID hashes. The scanner collects
   `.uid("...")` literals from the code and recomputes Flink's hash (murmur3_128, seed 0), so the
   report says `operator uid "alert-function"` and not a 32-character hex string.

## Not in v1

PyFlink and SQL scripts, remote savepoints (S3/HDFS), and automatic rewrites (planned as OpenRewrite
recipes).
