package io.github.prajwaljadhav01.flink2check.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The rule catalogue, loaded from the JSON files that {@code tools/gen_rules.py} and
 * {@code tools/gen_connectors.py} generate from the Flink release notes and Maven Central.
 */
public final class Rules {

    /** A class that no longer exists on Flink 2.x. */
    public record RemovedClass(String fqcn, String group, String origin, String movedTo) {
        public boolean moved() {
            return movedTo != null;
        }
    }

    /** A method, field or constructor removed from a class that still exists. */
    public record RemovedMember(String owner, String kind, String name, String signature, boolean overloadOnly) { }

    /** A removed configuration key. */
    public record RemovedConfigKey(String key, boolean stillReferencedIn2x) { }

    /** Guidance for a group of rules. */
    public record Group(String title, String fix, String link) { }

    /** What Maven Central knows about a Flink artifact. */
    public record Artifact(String artifactId, String latestFor1x, Map<String, String> flink2Releases, String note) {
        public boolean hasFlink2Release() {
            return !flink2Releases.isEmpty();
        }
    }

    private final Map<String, RemovedClass> removedClasses = new HashMap<>();
    private final Map<String, Map<String, RemovedClass>> removedByPackage = new HashMap<>();
    private final List<Map.Entry<String, String>> removedPackages = new ArrayList<>();
    private final Map<String, List<RemovedMember>> membersByOwner = new HashMap<>();
    private final Map<String, RemovedConfigKey> configKeys = new LinkedHashMap<>();
    private final Map<String, Group> groups = new HashMap<>();
    private final Map<String, Artifact> artifacts = new HashMap<>();
    private final Map<String, String> memberHints = new HashMap<>();
    private final String coreSource;
    private final String coreGeneratedOn;
    private final String connectorsGeneratedOn;

    private Rules(JsonNode core, JsonNode connectors) {
        coreSource = core.path("source").asText();
        coreGeneratedOn = core.path("generatedOn").asText();
        connectorsGeneratedOn = connectors.path("generatedOn").asText();

        core.path("groups").fields().forEachRemaining(e -> groups.put(e.getKey(), new Group(
                e.getValue().path("title").asText(),
                e.getValue().path("fix").asText(),
                e.getValue().path("link").asText())));
        groups.putIfAbsent("connector", new Group(
                "Connector class was removed in its Flink 2.x release",
                "Move to the connector's FLIP-27 Source / Sink V2 API (e.g. KafkaSource, KafkaSink).",
                "https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/"));

        for (JsonNode c : core.path("removedClasses")) {
            addRemovedClass(new RemovedClass(c.path("fqcn").asText(), c.path("group").asText(), "flink",
                    c.hasNonNull("movedTo") ? c.path("movedTo").asText().replace('$', '.') : null));
        }
        for (JsonNode p : core.path("removedPackages")) {
            removedPackages.add(Map.entry(p.path("prefix").asText(), p.path("group").asText()));
        }
        for (JsonNode m : core.path("removedMembers")) {
            RemovedMember rm = new RemovedMember(
                    m.path("owner").asText().replace('$', '.'),
                    m.path("kind").asText(),
                    m.path("name").asText(),
                    m.path("signature").asText(),
                    m.path("overloadOnly").asBoolean(false));
            membersByOwner.computeIfAbsent(rm.owner(), k -> new ArrayList<>()).add(rm);
        }
        core.path("memberHints").fields().forEachRemaining(e -> memberHints.put(e.getKey(), e.getValue().asText()));
        for (JsonNode k : core.path("removedConfigKeys")) {
            String key = k.isTextual() ? k.asText() : k.path("key").asText();
            configKeys.put(key, new RemovedConfigKey(key, k.path("stillReferencedIn2x").asBoolean(false)));
        }

        for (JsonNode a : connectors.path("artifacts")) {
            Map<String, String> releases = new LinkedHashMap<>();
            a.path("flink2Releases").fields().forEachRemaining(e -> releases.put(e.getKey(), e.getValue().asText()));
            String id = a.path("artifactId").asText();
            artifacts.put(id, new Artifact(id,
                    a.path("latestFor1x").isNull() ? null : a.path("latestFor1x").asText(null),
                    releases,
                    a.path("note").asText(null)));
            for (JsonNode cls : a.path("removedClassesIn2x")) {
                String fqcn = cls.asText();
                if (!removedClasses.containsKey(fqcn)) {
                    addRemovedClass(new RemovedClass(fqcn, "connector", id, null));
                }
            }
        }
    }

    private void addRemovedClass(RemovedClass rc) {
        if (rc.fqcn().endsWith("$")) {
            return; // Scala companion objects of planner internals; never referenced by user code
        }
        String dotted = rc.fqcn().replace('$', '.');
        RemovedClass normalised = new RemovedClass(dotted, rc.group(), rc.origin(), rc.movedTo());
        removedClasses.put(dotted, normalised);
        int i = topLevelEnd(rc.fqcn());
        String pkg = rc.fqcn().substring(0, rc.fqcn().lastIndexOf('.', i - 1));
        String simple = dotted.substring(pkg.length() + 1);
        removedByPackage.computeIfAbsent(pkg, k -> new HashMap<>()).put(simple, normalised);
    }

    private static int topLevelEnd(String fqcn) {
        int dollar = fqcn.indexOf('$');
        return dollar < 0 ? fqcn.length() : dollar;
    }

    public static Rules load() {
        ObjectMapper om = new ObjectMapper();
        try (InputStream core = Rules.class.getResourceAsStream("/rules/flink-core.json");
             InputStream conn = Rules.class.getResourceAsStream("/rules/connectors.json")) {
            if (core == null || conn == null) {
                throw new IllegalStateException("rule files missing from the jar");
            }
            return new Rules(om.readTree(core), om.readTree(conn));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Exact lookup of a class name; nested classes may use '.' or '$'. */
    public Optional<RemovedClass> removedClass(String fqcn) {
        return Optional.ofNullable(removedClasses.get(fqcn.replace('$', '.')));
    }

    /** Removed classes in a package, keyed by simple (possibly nested, dotted) name. */
    public Map<String, RemovedClass> removedInPackage(String pkg) {
        return removedByPackage.getOrDefault(pkg, Map.of());
    }

    /** A removed package prefix covering this name, if any; returns the group. */
    public Optional<String> removedPackageGroup(String name) {
        for (Map.Entry<String, String> e : removedPackages) {
            if (name.equals(e.getKey()) || name.startsWith(e.getKey() + ".")) {
                return Optional.of(e.getValue());
            }
        }
        return Optional.empty();
    }

    public List<RemovedMember> removedMembers(String ownerFqcn) {
        return membersByOwner.getOrDefault(ownerFqcn.replace('$', '.'), List.of());
    }

    public Set<String> ownersWithRemovedMembers() {
        return membersByOwner.keySet();
    }

    /** A specific replacement hint for a removed member, if one was written. */
    public Optional<String> memberHint(String ownerFqcn, String name) {
        String simple = ownerFqcn.substring(ownerFqcn.lastIndexOf('.') + 1);
        return Optional.ofNullable(memberHints.get(simple + "#" + name));
    }

    public Optional<RemovedConfigKey> removedConfigKey(String key) {
        return Optional.ofNullable(configKeys.get(key));
    }

    public Group group(String name) {
        return groups.getOrDefault(name, groups.get("other"));
    }

    public Optional<Artifact> artifact(String artifactId) {
        return Optional.ofNullable(artifacts.get(artifactId));
    }

    public String coreSource() {
        return coreSource;
    }

    public String coreGeneratedOn() {
        return coreGeneratedOn;
    }

    public String connectorsGeneratedOn() {
        return connectorsGeneratedOn;
    }

    public int removedClassCount() {
        return removedClasses.size();
    }

    public int removedMemberCount() {
        return membersByOwner.values().stream().mapToInt(List::size).sum();
    }

    public int removedConfigKeyCount() {
        return configKeys.size();
    }
}
