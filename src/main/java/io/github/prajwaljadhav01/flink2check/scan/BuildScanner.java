package io.github.prajwaljadhav01.flink2check.scan;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import io.github.prajwaljadhav01.flink2check.rules.Rules;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Checks Maven and Gradle build files: Flink version, connector versions, removed modules, Java level. */
public final class BuildScanner {

    private static final String RELEASE_NOTES =
            "https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/";
    private static final String CONNECTORS_DOC =
            "https://flink.apache.org/downloads/#apache-flink-connectors";

    /** Core modules whose version tells us which Flink the job is built against. */
    private static final Set<String> CORE_MODULES = Set.of(
            "flink-streaming-java", "flink-core", "flink-clients", "flink-runtime", "flink-java",
            "flink-table-api-java", "flink-table-api-java-bridge", "flink-table-runtime",
            "flink-table-planner-loader", "flink-table-planner_2.12");

    private static final Pattern PROPERTY = Pattern.compile("\\$\\{([^}]+)}");
    private static final Pattern GRADLE_COORD = Pattern.compile(
            "[\"']org\\.apache\\.flink:([\\w.\\-]+):([^\"'@]+)[\"']");
    private static final Pattern GRADLE_JAVA = Pattern.compile(
            "(?:sourceCompatibility|targetCompatibility|languageVersion\\s*=\\s*JavaLanguageVersion\\.of)\\s*[=(]?\\s*['\"]?(?:JavaVersion\\.VERSION_)?(1[._]8|\\d+)");

    private final Rules rules;
    private final Report report;
    private final String target;

    public BuildScanner(Rules rules, Report report, String target) {
        this.rules = rules;
        this.report = report;
        this.target = target;
    }

    public int scan(Path root) {
        List<Path> poms = ProjectFiles.find(root, p -> ProjectFiles.nameOf(p).equals("pom.xml"));
        for (Path pom : poms) {
            scanPom(root, pom);
        }
        List<Path> gradle = ProjectFiles.find(root, p -> {
            String n = ProjectFiles.nameOf(p);
            return n.equals("build.gradle") || n.equals("build.gradle.kts");
        });
        for (Path g : gradle) {
            scanGradle(root, g);
        }
        return poms.size() + gradle.size();
    }

    // ---------------------------------------------------------------- Maven

    private void scanPom(Path root, Path pom) {
        String where = ProjectFiles.display(root, pom);
        String text;
        Document doc;
        try {
            text = Files.readString(pom, StandardCharsets.UTF_8);
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder b = f.newDocumentBuilder();
            doc = b.parse(pom.toFile());
        } catch (Exception e) {
            report.add(new Finding(Severity.INFO, "build", "parse-error", "Could not parse pom.xml",
                    e.getMessage(), where, 0, "", ""));
            return;
        }
        Element project = doc.getDocumentElement();
        Map<String, String> props = new HashMap<>();
        Element propsEl = child(project, "properties");
        if (propsEl != null) {
            for (Element p : children(propsEl)) {
                props.put(p.getTagName(), p.getTextContent().trim());
            }
        }
        Element version = child(project, "version");
        if (version != null) {
            props.put("project.version", version.getTextContent().trim());
        }

        List<Element> deps = new ArrayList<>();
        collectDependencies(child(project, "dependencies"), deps);
        Element dm = child(project, "dependencyManagement");
        if (dm != null) {
            collectDependencies(child(dm, "dependencies"), deps);
        }

        for (Element dep : deps) {
            String g = resolve(text(dep, "groupId"), props);
            String a = resolve(text(dep, "artifactId"), props);
            String v = resolve(text(dep, "version"), props);
            if (a == null) {
                continue;
            }
            int line = lineOf(text, "<artifactId>" + text(dep, "artifactId") + "</artifactId>");
            checkDependency(g, a, v, where, line);
        }

        checkJavaLevel(props, project, text, where);
    }

    private void checkJavaLevel(Map<String, String> props, Element project, String text, String where) {
        String level = null;
        String key = null;
        for (String k : List.of("maven.compiler.release", "maven.compiler.target", "maven.compiler.source",
                "java.version", "jdk.version", "target.java.version")) {
            if (props.containsKey(k)) {
                level = resolve(props.get(k), props);
                key = "<" + k + ">";
                break;
            }
        }
        if (level == null) {
            for (String tag : List.of("release", "target", "source")) {
                int i = text.indexOf("<" + tag + ">");
                if (i >= 0 && text.lastIndexOf("maven-compiler-plugin", i) >= 0) {
                    level = text.substring(i + tag.length() + 2, text.indexOf("</" + tag + ">", i)).trim();
                    level = resolve(level, props);
                    key = "<" + tag + ">";
                    break;
                }
            }
        }
        if (level != null) {
            javaLevelFinding(level, where, lineOf(text, key));
        }
    }

    private static void collectDependencies(Element depsEl, List<Element> out) {
        if (depsEl == null) {
            return;
        }
        for (Element d : children(depsEl)) {
            if (d.getTagName().equals("dependency")) {
                out.add(d);
            }
        }
    }

    // ---------------------------------------------------------------- Gradle

    private void scanGradle(Path root, Path file) {
        String where = ProjectFiles.display(root, file);
        List<String> lines;
        Properties gradleProps = new Properties();
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            Path propsFile = file.resolveSibling("gradle.properties");
            if (Files.exists(propsFile)) {
                try (var r = Files.newBufferedReader(propsFile)) {
                    gradleProps.load(r);
                }
            }
        } catch (IOException e) {
            report.add(new Finding(Severity.INFO, "build", "parse-error", "Could not read Gradle file",
                    e.getMessage(), where, 0, "", ""));
            return;
        }
        Map<String, String> props = new HashMap<>();
        gradleProps.forEach((k, v) -> props.put(k.toString(), v.toString()));
        Pattern def = Pattern.compile("(?:def|val|var)?\\s*(\\w+)\\s*=\\s*[\"']([^\"']+)[\"']");
        for (String l : lines) {
            Matcher m = def.matcher(l.trim());
            if (m.matches()) {
                props.putIfAbsent(m.group(1), m.group(2));
            }
        }
        for (int i = 0; i < lines.size(); i++) {
            Matcher m = GRADLE_COORD.matcher(lines.get(i));
            while (m.find()) {
                String v = m.group(2).replaceAll("^\\$\\{?(\\w+)}?$", "\\${$1}");
                checkDependency("org.apache.flink", m.group(1), resolve(v, props), where, i + 1);
            }
            Matcher j = GRADLE_JAVA.matcher(lines.get(i));
            if (j.find()) {
                javaLevelFinding(j.group(1).replace('_', '.'), where, i + 1);
            }
        }
    }

    // ---------------------------------------------------------------- rules

    private void checkDependency(String groupId, String artifactId, String version, String where, int line) {
        if (groupId == null) {
            return;
        }
        if (!groupId.equals("org.apache.flink")) {
            if (artifactId.toLowerCase().contains("flink")) {
                report.add(new Finding(Severity.WARNING, "build", "third-party-flink-library",
                        "Third-party Flink library: check it has a Flink 2.x build",
                        groupId + ":" + artifactId + (version != null ? ":" + version : ""),
                        where, line,
                        "Look for a release built against Flink 2.x; libraries using SourceFunction/SinkFunction will not load.",
                        RELEASE_NOTES));
            }
            return;
        }
        if (CORE_MODULES.contains(artifactId) && version != null && version.matches("\\d+\\.\\d+.*")) {
            report.fact("flinkVersion", version);
        }
        var artifact = rules.artifact(artifactId);
        if (artifact.isEmpty()) {
            if (version != null && version.matches(".*-1\\.\\d+$")) {
                report.add(new Finding(Severity.WARNING, "build", "connector-unknown",
                        "Connector built for Flink 1.x; check for a 2.x release",
                        artifactId + ":" + version, where, line,
                        "Find a release of this connector whose version ends in -2.x.", CONNECTORS_DOC));
            }
            return;
        }
        Rules.Artifact a = artifact.get();
        String coord = artifactId + (version != null ? ":" + version : "");
        if (version != null && version.startsWith("2.") || version != null && version.matches(".*-2\\.\\d+$")) {
            return; // already on a 2.x line
        }
        if (!a.hasFlink2Release()) {
            report.add(new Finding(Severity.ERROR, "build", "no-flink2-release",
                    "No release for Flink 2.x (as of " + rules.connectorsGeneratedOn() + ")",
                    coord, where, line,
                    a.note() != null ? a.note()
                            : "Replace it, wait for a 2.x release, or keep this job on Flink 1.20 LTS for now.",
                    CONNECTORS_DOC));
            return;
        }
        String suggestion = pickRelease(a.flink2Releases());
        boolean isCore = !(a.latestFor1x() != null && a.latestFor1x().matches(".*-1\\.\\d+$"));
        report.add(new Finding(Severity.WARNING, "build",
                isCore ? "bump-core-version" : "bump-connector-version",
                isCore ? "Flink module must move to a 2.x version" : "Connector needs its Flink 2.x release",
                coord + " -> " + artifactId + ":" + suggestion,
                where, line,
                isCore ? "Set the Flink version to " + suggestion + " for all Flink modules."
                        : "Use " + suggestion + "; its major version changed, so check its release notes for removed APIs.",
                isCore ? RELEASE_NOTES : CONNECTORS_DOC));
    }

    private String pickRelease(Map<String, String> releases) {
        if (releases.containsKey(target)) {
            return releases.get(target);
        }
        String best = null;
        for (Map.Entry<String, String> e : releases.entrySet()) {
            if (e.getKey().compareTo(target) <= 0) {
                best = e.getValue();
            }
        }
        if (best == null) {
            best = releases.values().iterator().next();
        }
        return best + (releases.containsKey(target) ? "" : " (newest release for Flink <= " + target + ")");
    }

    private void javaLevelFinding(String level, String where, int line) {
        int major;
        try {
            String l = level.startsWith("1.") ? level.substring(2) : level;
            major = Integer.parseInt(l.replaceAll("[^0-9].*", ""));
        } catch (NumberFormatException e) {
            return;
        }
        report.fact("javaLevel", String.valueOf(major));
        if (major < 11) {
            report.add(new Finding(Severity.ERROR, "build", "java-version",
                    "Java " + major + " is not supported by Flink 2.x", "Build targets Java " + major,
                    where, line, "Build for Java 17 (the Flink 2.x default); Java 11 is the minimum.", RELEASE_NOTES));
        } else if (major < 17) {
            report.add(new Finding(Severity.WARNING, "build", "java-version",
                    "Flink 2.x defaults to Java 17", "Build targets Java " + major,
                    where, line, "Move to Java 17; Flink 2.x images and docs assume it.", RELEASE_NOTES));
        }
    }

    // ---------------------------------------------------------------- helpers

    private static String resolve(String value, Map<String, String> props) {
        if (value == null) {
            return null;
        }
        String v = value;
        for (int i = 0; i < 5 && v.contains("${"); i++) {
            Matcher m = PROPERTY.matcher(v);
            StringBuilder sb = new StringBuilder();
            while (m.find()) {
                String rep = props.getOrDefault(m.group(1), m.group(0));
                m.appendReplacement(sb, Matcher.quoteReplacement(rep));
            }
            m.appendTail(sb);
            if (sb.toString().equals(v)) {
                break;
            }
            v = sb.toString();
        }
        return v;
    }

    private static int lineOf(String text, String needle) {
        if (needle == null) {
            return 0;
        }
        int i = text.indexOf(needle);
        if (i < 0) {
            return 0;
        }
        int line = 1;
        for (int k = 0; k < i; k++) {
            if (text.charAt(k) == '\n') {
                line++;
            }
        }
        return line;
    }

    private static Element child(Element parent, String name) {
        if (parent == null) {
            return null;
        }
        for (Element e : children(parent)) {
            if (e.getTagName().equals(name)) {
                return e;
            }
        }
        return null;
    }

    private static List<Element> children(Element parent) {
        List<Element> out = new ArrayList<>();
        NodeList nl = parent.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n instanceof Element e) {
                out.add(e);
            }
        }
        return out;
    }

    private static String text(Element parent, String name) {
        Element e = child(parent, name);
        return e == null ? null : e.getTextContent().trim();
    }
}
