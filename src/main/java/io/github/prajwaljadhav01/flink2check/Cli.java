package io.github.prajwaljadhav01.flink2check;

import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;
import io.github.prajwaljadhav01.flink2check.report.Reporters;
import io.github.prajwaljadhav01.flink2check.rules.Rules;
import io.github.prajwaljadhav01.flink2check.scan.BuildScanner;
import io.github.prajwaljadhav01.flink2check.scan.ConfigScanner;
import io.github.prajwaljadhav01.flink2check.scan.SavepointScanner;
import io.github.prajwaljadhav01.flink2check.scan.SourceScanner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "flink2-check",
        mixinStandardHelpOptions = true,
        version = "flink2-check 0.1.0",
        description = {
                "Finds what will break when a Flink 1.x job moves to Flink 2.x.",
                "Checks Java/Scala code, pom.xml/build.gradle, Flink config files and, optionally, a savepoint."
        },
        footer = {
                "",
                "Exit codes: 0 = nothing at or above --fail-on, 1 = findings at or above --fail-on, 2 = usage or I/O error.",
                "",
                "Examples:",
                "  flink2-check ./my-flink-job",
                "  flink2-check ./my-flink-job --savepoint ./savepoint-1a2b3c --job-jar target/my-job.jar",
                "  flink2-check ./my-flink-job --format markdown --output flink2-report.md"
        })
public final class Cli implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1", paramLabel = "PROJECT",
            description = "Project directory (or a single file) to scan. Omit to scan only a savepoint.")
    Path project;

    @Option(names = {"-s", "--savepoint"}, paramLabel = "DIR",
            description = "Savepoint or retained checkpoint directory (or its _metadata file), on local disk.")
    Path savepoint;

    @Option(names = {"-j", "--job-jar"}, paramLabel = "JAR",
            description = "Job jar(s) holding the classes stored in state (POJOs, Avro types). Repeatable.")
    List<Path> jobJars = new ArrayList<>();

    @Option(names = "--uid", paramLabel = "UID",
            description = "Operator uid(s) to name savepoint operators by. Uids set with .uid(\"...\") in the scanned code are picked up automatically.")
    List<String> extraUids = new ArrayList<>();

    @Option(names = {"-t", "--target"}, defaultValue = "2.2",
            description = "Flink version you are moving to: 2.0, 2.1, 2.2 (default: ${DEFAULT-VALUE}).")
    String target;

    @Option(names = {"-f", "--format"}, defaultValue = "text",
            description = "Output: text, markdown or json (default: ${DEFAULT-VALUE}).")
    String format;

    @Option(names = {"-o", "--output"}, paramLabel = "FILE", description = "Write the report to a file.")
    Path output;

    @Option(names = "--fail-on", defaultValue = "error",
            description = "Exit 1 when a finding is at least: error, warning, info or never (default: ${DEFAULT-VALUE}).")
    String failOn;

    @Option(names = "--no-color", description = "Plain text output.")
    boolean noColor;

    @Override
    public Integer call() throws Exception {
        if (project == null && savepoint == null) {
            System.err.println("Nothing to scan: give a PROJECT directory and/or --savepoint.");
            return 2;
        }
        Rules rules = Rules.load();
        Report report = new Report(target);
        java.util.Set<String> uids = new java.util.TreeSet<>(extraUids);

        if (project != null) {
            if (!Files.exists(project)) {
                System.err.println("No such path: " + project);
                return 2;
            }
            Path root = project.toAbsolutePath().normalize();
            report.fact("project", project.toString());
            SourceScanner sources = new SourceScanner(rules, report);
            int code = sources.scan(root);
            uids.addAll(sources.uids());
            int builds = new BuildScanner(rules, report, target).scan(root);
            int configs = new ConfigScanner(rules, report).scan(root);
            report.fact("scanned", code + " source files, " + builds + " build files, " + configs + " config files");
        }
        if (savepoint != null) {
            try {
                SavepointScanner.Summary s = new SavepointScanner(report, jobJars, uids).scan(savepoint);
                report.fact("state", s.states() + " states in " + s.operators() + " operators, "
                        + s.atRisk() + " at risk" + (s.unreadable() > 0 ? ", " + s.unreadable() + " unreadable" : ""));
            } catch (Exception e) {
                report.add(new Finding(Severity.ERROR, "state", "savepoint-unreadable",
                        "Could not read the savepoint", e.getClass().getSimpleName() + ": " + e.getMessage(),
                        savepoint.toString(), 0,
                        "Point --savepoint at the directory that holds _metadata, on local disk; add --job-jar if it needs job classes.",
                        ""));
            }
        }
        report.fact("rules", rules.removedClassCount() + " removed classes, " + rules.removedMemberCount()
                + " removed members, " + rules.removedConfigKeyCount() + " config keys (Flink 2.0 release notes); "
                + "connector catalogue from Maven Central, " + rules.connectorsGeneratedOn());

        boolean color = !noColor && output == null && System.console() != null && format.equals("text");
        String rendered = Reporters.render(report, format, color);
        if (output != null) {
            Files.writeString(output, rendered, StandardCharsets.UTF_8);
            System.err.println("Report written to " + output + " (" + report.verdict() + ")");
        } else {
            System.out.print(rendered);
        }
        return exitCode(report);
    }

    private int exitCode(Report report) {
        Severity threshold = switch (failOn.toLowerCase()) {
            case "never" -> null;
            case "warning", "warn" -> Severity.WARNING;
            case "info" -> Severity.INFO;
            default -> Severity.ERROR;
        };
        if (threshold == null) {
            return 0;
        }
        for (Finding f : report.findings()) {
            if (f.severity().compareTo(threshold) <= 0) {
                return 1;
            }
        }
        return 0;
    }

    public static void main(String[] args) {
        int code = new CommandLine(new Cli()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
        System.exit(code);
    }
}
