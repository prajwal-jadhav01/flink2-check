package io.github.prajwaljadhav01.flink2check.scan;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reads a Flink 1.x savepoint or retained checkpoint and finds state that Flink 2.x cannot restore.
 *
 * <p>It never reads the state values. For every operator it opens only the small header that
 * lists each state's name and the snapshot of the serializer that wrote it, then walks that
 * serializer tree. Kryo-serialised state (generic types, POJOs with collection fields, most Java
 * records and third-party types) cannot be read by Flink 2.x, which moved from Kryo 2 to Kryo 5 and
 * added dedicated List/Map/Set serializers.
 */
public final class SavepointScanner {

    private static final String STATE_DOC =
            "https://docs.aws.amazon.com/managed-flink/latest/java/state-compatibility.html";
    private static final String KRYO_FIX =
            "Before upgrading, either rebuild this state from the source on Flink 2.x (start without the savepoint "
                    + "for this operator, or with a new uid), or migrate it on 1.20 to a Flink-native type "
                    + "(POJO without collection fields, Avro, Protobuf) with the State Processor API.";

    /** Operator-state names written by legacy connectors that the new sources cannot read. */
    private static final Map<String, String> LEGACY_OPERATOR_STATE = Map.of(
            "topic-partition-offset-states", "FlinkKafkaConsumer",
            "next-transactional-id-hint", "FlinkKafkaProducer",
            "next-transactional-id-hint-v2", "FlinkKafkaProducer",
            "Kinesis-Stream-Shard-State", "FlinkKinesisConsumer");

    private final Report report;
    private final ClassLoader classLoader;
    private final Map<String, String> uidByOperatorId = new java.util.HashMap<>();

    public SavepointScanner(Report report, List<Path> jobJars) {
        this(report, jobJars, java.util.Set.of());
    }

    /**
     * @param knownUids operator uids (from {@code .uid("...")} in the code or {@code --uid}), so findings
     *                  can name the operator instead of showing its hash
     */
    public SavepointScanner(Report report, List<Path> jobJars, java.util.Set<String> knownUids) {
        this.report = report;
        for (String uid : knownUids) {
            uidByOperatorId.put(operatorIdForUid(uid), uid);
        }
        URL[] urls = jobJars.stream().map(p -> {
            try {
                return p.toUri().toURL();
            } catch (IOException e) {
                throw new IllegalArgumentException(p.toString(), e);
            }
        }).toArray(URL[]::new);
        this.classLoader = new URLClassLoader(urls, SavepointScanner.class.getClassLoader());
    }

    /** Result counters, used for the summary line. */
    public record Summary(int operators, int states, int atRisk, int unreadable) { }

    public Summary scan(Path savepoint) throws IOException {
        Path metadataFile = Files.isDirectory(savepoint) ? savepoint.resolve("_metadata") : savepoint;
        Path dir = metadataFile.getParent();
        if (!Files.exists(metadataFile)) {
            throw new IOException("No _metadata file at " + metadataFile
                    + ". Pass the savepoint directory (copy it locally first if it is on S3/HDFS).");
        }
        CheckpointMetadata metadata;
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try (InputStream in = Files.newInputStream(metadataFile)) {
            metadata = Checkpoints.loadCheckpointMetadata(
                    new DataInputStream(in), classLoader, dir.toAbsolutePath().toUri().toString());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        report.fact("savepoint", savepoint.toString());
        report.fact("savepointCheckpointId", String.valueOf(metadata.getCheckpointId()));

        int operators = 0;
        int states = 0;
        int atRisk = 0;
        int unreadable = 0;
        for (OperatorState op : metadata.getOperatorStates()) {
            String opId = op.getOperatorID().toString();
            List<StateMetaInfoSnapshot> metas = new ArrayList<>();
            boolean hasState = false;
            Optional<String> readError = Optional.empty();

            KeyedStateHandle keyed = firstKeyedHandle(op);
            if (keyed != null) {
                hasState = true;
                try {
                    metas.addAll(readKeyedMeta(keyed));
                } catch (Exception e) {
                    readError = Optional.of(describe(e));
                }
            }
            OperatorStateHandle operatorHandle = firstOperatorHandle(op);
            if (operatorHandle != null) {
                hasState = true;
                try {
                    metas.addAll(readOperatorMeta(operatorHandle));
                } catch (Exception e) {
                    readError = Optional.of(describe(e));
                }
            }
            if (!hasState) {
                continue;
            }
            operators++;
            String location = uidByOperatorId.containsKey(opId)
                    ? "operator uid \"" + uidByOperatorId.get(opId) + "\""
                    : "operator " + opId;
            if (readError.isPresent()) {
                unreadable++;
                report.add(new Finding(Severity.WARNING, "state", "state-unreadable",
                        "Could not read this operator's state header", readError.get(), location, 0,
                        "Pass the job jar with --job-jar so the scanner can load the job's own classes.", STATE_DOC));
            }
            for (StateMetaInfoSnapshot meta : metas) {
                states++;
                String stateName = meta.getName();
                String stateLoc = location + " / state \"" + stateName + "\"";
                if (LEGACY_OPERATOR_STATE.containsKey(stateName)) {
                    atRisk++;
                    report.add(new Finding(Severity.ERROR, "state", "legacy-connector-state",
                            "State written by " + LEGACY_OPERATOR_STATE.get(stateName) + ", which Flink 2.x removed",
                            meta.getBackendStateType() + " state \"" + stateName + "\"", stateLoc, 0,
                            "The new KafkaSource/KafkaSink cannot restore it. Give the new source a new uid and start it "
                                    + "from the consumer group's committed offsets (allowNonRestoredState).",
                            "https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/"));
                    continue;
                }
                List<String> problems = new ArrayList<>();
                for (Map.Entry<String, TypeSerializerSnapshot<?>> e : meta.getSerializerSnapshotsImmutable().entrySet()) {
                    walk(e.getValue(), role(e.getKey()), problems, new IdentityHashMap<>(), 0);
                }
                if (!problems.isEmpty()) {
                    atRisk++;
                    report.add(new Finding(Severity.ERROR, "state", "kryo-state",
                            "State uses Kryo; Flink 2.x cannot restore it",
                            meta.getBackendStateType() + " state \"" + stateName + "\": " + String.join("; ", problems),
                            stateLoc, 0, KRYO_FIX, STATE_DOC));
                }
            }
        }
        report.fact("savepointOperatorsWithState", String.valueOf(operators));
        report.fact("savepointStates", String.valueOf(states));
        if (atRisk == 0 && unreadable == 0 && states > 0) {
            report.add(new Finding(Severity.INFO, "state", "state-ok",
                    "No Kryo or legacy-connector state found",
                    states + " states in " + operators + " operators use Flink-native serializers",
                    savepoint.toString(), 0,
                    "SQL/Table state is compatible with caveats; still test the restore on a copy before production.",
                    STATE_DOC));
        }
        return new Summary(operators, states, atRisk, unreadable);
    }

    /** Same hash Flink 1.x uses for a user-set uid (StreamGraphHasherV2: murmur3_128, seed 0). */
    static String operatorIdForUid(String uid) {
        byte[] hash = org.apache.flink.shaded.guava31.com.google.common.hash.Hashing.murmur3_128(0)
                .newHasher()
                .putString(uid, java.nio.charset.StandardCharsets.UTF_8)
                .hash()
                .asBytes();
        return new org.apache.flink.runtime.jobgraph.OperatorID(hash).toString();
    }

    private static KeyedStateHandle firstKeyedHandle(OperatorState op) {
        for (OperatorSubtaskState s : op.getStates()) {
            for (KeyedStateHandle h : s.getManagedKeyedState()) {
                return h;
            }
        }
        return null;
    }

    private static OperatorStateHandle firstOperatorHandle(OperatorState op) {
        for (OperatorSubtaskState s : op.getStates()) {
            for (OperatorStateHandle h : s.getManagedOperatorState()) {
                return h;
            }
        }
        return null;
    }

    private List<StateMetaInfoSnapshot> readKeyedMeta(KeyedStateHandle handle) throws IOException {
        StreamStateHandle stream;
        if (handle instanceof KeyGroupsStateHandle kg) {
            stream = kg; // canonical format: the header sits at the start of the stream
        } else if (handle instanceof IncrementalKeyedStateHandle inc) {
            stream = inc.getMetaDataStateHandle(); // RocksDB native / incremental
        } else {
            throw new IOException("unsupported keyed state handle " + handle.getClass().getSimpleName()
                    + " (changelog state backend?)");
        }
        try (FSDataInputStream in = stream.openInputStream()) {
            KeyedBackendSerializationProxy<?> proxy = new KeyedBackendSerializationProxy<>(classLoader);
            withClassLoader(() -> proxy.read(new DataInputViewStreamWrapper(in)));
            List<StateMetaInfoSnapshot> out = new ArrayList<>(proxy.getStateMetaInfoSnapshots());
            // The key serializer matters too: a Kryo key breaks every keyed state of the operator.
            List<String> keyProblems = new ArrayList<>();
            walk(proxy.getKeySerializerSnapshot(), "key", keyProblems, new IdentityHashMap<>(), 0);
            if (!keyProblems.isEmpty()) {
                out.add(new StateMetaInfoSnapshot("(key of keyed state)",
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE, Map.of(),
                        Map.of("KEY_SERIALIZER", proxy.getKeySerializerSnapshot())));
            }
            return out;
        }
    }

    private List<StateMetaInfoSnapshot> readOperatorMeta(OperatorStateHandle handle) throws IOException {
        try (FSDataInputStream in = handle.openInputStream()) {
            OperatorBackendSerializationProxy proxy = new OperatorBackendSerializationProxy(classLoader);
            withClassLoader(() -> proxy.read(new DataInputViewStreamWrapper(in)));
            List<StateMetaInfoSnapshot> out = new ArrayList<>(proxy.getOperatorStateMetaInfoSnapshots());
            out.addAll(proxy.getBroadcastStateMetaInfoSnapshots());
            return out;
        }
    }

    private interface IoAction {
        void run() throws IOException;
    }

    private void withClassLoader(IoAction action) throws IOException {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            action.run();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    // ------------------------------------------------------------ serializer tree walk

    /**
     * Depth-first walk over a serializer snapshot and everything it holds (nested snapshots of
     * composite serializers, POJO field snapshots, subclass snapshots). Uses reflection because
     * many snapshot classes keep their children private.
     */
    private void walk(Object node, String path, List<String> problems, Map<Object, Boolean> seen, int depth) {
        if (node == null || depth > 25 || seen.put(node, Boolean.TRUE) != null) {
            return;
        }
        if (node instanceof TypeSerializerSnapshot<?> snap) {
            String cls = snap.getClass().getName();
            if (cls.endsWith("KryoSerializerSnapshot")) {
                problems.add(path + " is Kryo-serialised" + kryoType(snap).map(t -> " (" + t + ")").orElse(""));
                return;
            }
            if (cls.startsWith("org.apache.flink.api.scala.") || cls.contains("ScalaCaseClassSerializer")
                    || cls.contains("CaseClassSerializer") || cls.contains(".scala.")) {
                problems.add(path + " uses a Scala serializer (" + snap.getClass().getSimpleName()
                        + "); the Scala API is gone in 2.x");
                return;
            }
        }
        if (node instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                walk(e.getValue(), path + "." + label(e.getKey()), problems, seen, depth + 1);
            }
            return;
        }
        if (node instanceof Collection<?> c) {
            int i = 0;
            for (Object o : c) {
                walk(o, path + "[" + i++ + "]", problems, seen, depth + 1);
            }
            return;
        }
        if (node instanceof Object[] arr) {
            for (int i = 0; i < arr.length; i++) {
                walk(arr[i], path + "[" + i + "]", problems, seen, depth + 1);
            }
            return;
        }
        if (node instanceof Optional<?> o) {
            o.ifPresent(v -> walk(v, path, problems, seen, depth + 1));
            return;
        }
        Class<?> type = node.getClass();
        if (!type.getName().startsWith("org.apache.flink.")) {
            return; // only descend into Flink's own holder objects
        }
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()
                        || f.getType() == String.class || f.getType() == Class.class
                        || Field.class.isAssignableFrom(f.getType())) {
                    continue;
                }
                try {
                    f.setAccessible(true);
                    Object child = f.get(node);
                    walk(child, path, problems, seen, depth + 1);
                } catch (ReflectiveOperationException | RuntimeException ignored) {
                    // inaccessible field; skip it
                }
            }
        }
    }

    private static String label(Object key) {
        if (key instanceof Field f) {
            return f.getName();
        }
        if (key instanceof Class<?> c) {
            return c.getSimpleName();
        }
        return String.valueOf(key);
    }

    private static Optional<String> kryoType(Object kryoSnapshot) {
        try {
            Field data = kryoSnapshot.getClass().getDeclaredField("snapshotData");
            data.setAccessible(true);
            Object d = data.get(kryoSnapshot);
            for (Field f : d.getClass().getDeclaredFields()) {
                if (f.getType() == Class.class) {
                    f.setAccessible(true);
                    Object c = f.get(d);
                    if (c instanceof Class<?> cls) {
                        return Optional.of(cls.getName());
                    }
                }
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // fall through
        }
        return Optional.empty();
    }

    private static String role(String serializerKey) {
        return switch (serializerKey) {
            case "VALUE_SERIALIZER" -> "value";
            case "KEY_SERIALIZER" -> "key";
            case "NAMESPACE_SERIALIZER" -> "namespace";
            default -> serializerKey.toLowerCase();
        };
    }

    private static String describe(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getClass().getSimpleName() + ": " + root.getMessage();
        if (root instanceof ClassNotFoundException) {
            msg = "class " + root.getMessage() + " not found";
        }
        return msg;
    }
}
