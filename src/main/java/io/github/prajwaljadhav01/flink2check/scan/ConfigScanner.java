package io.github.prajwaljadhav01.flink2check.scan;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import io.github.prajwaljadhav01.flink2check.rules.Rules;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks Flink configuration: legacy {@code flink-conf.yaml}, the new {@code config.yaml}, and the
 * {@code flinkConfiguration} block of Flink Kubernetes Operator {@code FlinkDeployment} resources.
 */
public final class ConfigScanner {

    private static final String MIGRATION_DOC =
            "https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/config/#migrate-from-flink-confyaml-to-configyaml";

    private final Rules rules;
    private final Report report;

    public ConfigScanner(Rules rules, Report report) {
        this.rules = rules;
        this.report = report;
    }

    public int scan(Path root) {
        List<Path> files = ProjectFiles.find(root, p -> {
            String n = ProjectFiles.nameOf(p);
            return n.endsWith(".yaml") || n.endsWith(".yml");
        });
        int checked = 0;
        for (Path f : files) {
            String name = ProjectFiles.nameOf(f);
            String where = ProjectFiles.display(root, f);
            String text;
            try {
                text = Files.readString(f, StandardCharsets.UTF_8);
            } catch (IOException e) {
                continue;
            }
            if (name.equals("flink-conf.yaml")) {
                checked++;
                scanLegacy(text, where);
            } else if (name.equals("config.yaml")) {
                checked++;
                scanYaml(text, where, flattenYaml(text));
            } else if (text.contains("kind: FlinkDeployment") || text.contains("kind: FlinkSessionJob")) {
                checked++;
                scanFlinkDeployment(text, where);
            }
        }
        return checked;
    }

    private void scanLegacy(String text, String where) {
        report.add(new Finding(Severity.ERROR, "config", "legacy-flink-conf",
                "flink-conf.yaml is no longer read by Flink 2.x",
                "Flink 2.x only reads config.yaml (standard YAML).", where, 1,
                "Convert it with bin/migrate-config-file.sh from a Flink 1.19+/1.20 distribution, then rename to config.yaml.",
                MIGRATION_DOC));
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String l = lines[i].trim();
            if (l.isEmpty() || l.startsWith("#")) {
                continue;
            }
            int colon = l.indexOf(':');
            if (colon > 0) {
                checkKey(l.substring(0, colon).trim(), where, i + 1);
            }
        }
    }

    private void scanYaml(String text, String where, Map<String, Object> flat) {
        for (String key : flat.keySet()) {
            checkKey(key, where, lineOfKey(text, key));
        }
    }

    @SuppressWarnings("unchecked")
    private void scanFlinkDeployment(String text, String where) {
        for (Object docObj : yaml().loadAll(text)) {
            if (!(docObj instanceof Map<?, ?> doc)) {
                continue;
            }
            Object spec = doc.get("spec");
            if (!(spec instanceof Map<?, ?> specMap)) {
                continue;
            }
            Object version = specMap.get("flinkVersion");
            if (version != null && version.toString().startsWith("v1_")) {
                report.fact("operatorFlinkVersion", version.toString().substring(1).replace('_', '.'));
                report.add(new Finding(Severity.INFO, "config", "operator-flink-version",
                        "FlinkDeployment runs Flink " + version.toString().substring(1).replace('_', '.'),
                        "spec.flinkVersion: " + version, where, lineOfKey(text, "flinkVersion"),
                        "Change spec.flinkVersion and spec.image together when you upgrade; take a savepoint first.",
                        "https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/job-management/"));
            }
            Object conf = specMap.get("flinkConfiguration");
            if (conf instanceof Map<?, ?> confMap) {
                Map<String, Object> flat = new LinkedHashMap<>();
                flatten("", (Map<String, Object>) confMap, flat);
                scanYaml(text, where, flat);
            }
        }
    }

    private void checkKey(String key, String where, int line) {
        rules.removedConfigKey(key).ifPresent(k -> report.add(new Finding(
                k.stillReferencedIn2x() ? Severity.INFO : Severity.ERROR,
                "config",
                "removed-config/" + k.key(),
                k.stillReferencedIn2x()
                        ? "Listed as removed in the 2.0 notes, but 2.0 still recognises this key"
                        : "Config option was removed in Flink 2.0",
                k.key(), where, line,
                k.stillReferencedIn2x() ? "Check the Flink 2.x docs for this key; it may now be a fallback for a new name."
                        : rules.group("config").fix(),
                rules.group("config").link())));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> flattenYaml(String text) {
        Map<String, Object> flat = new LinkedHashMap<>();
        try {
            Object root = yaml().load(text);
            if (root instanceof Map<?, ?> m) {
                flatten("", (Map<String, Object>) m, flat);
            }
        } catch (RuntimeException ignored) {
            // not valid YAML; nothing to check
        }
        return flat;
    }

    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> in, Map<String, Object> out) {
        for (Map.Entry<String, Object> e : in.entrySet()) {
            String key = prefix.isEmpty() ? String.valueOf(e.getKey()) : prefix + "." + e.getKey();
            if (e.getValue() instanceof Map<?, ?> nested) {
                flatten(key, (Map<String, Object>) nested, out);
            } else {
                out.put(key, e.getValue());
            }
        }
    }

    private static Yaml yaml() {
        return new Yaml(new SafeConstructor(new LoaderOptions()));
    }

    /** Best-effort line of a (possibly nested) key: the line of its last segment, or of the flat key. */
    private static int lineOfKey(String text, String key) {
        String[] lines = text.split("\n", -1);
        String last = key.substring(key.lastIndexOf('.') + 1);
        for (int i = 0; i < lines.length; i++) {
            String l = lines[i].trim();
            if (l.startsWith(key + ":") || l.startsWith("\"" + key + "\":")) {
                return i + 1;
            }
        }
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].trim().startsWith(last + ":")) {
                return i + 1;
            }
        }
        return 0;
    }
}
