package io.github.prajwaljadhav01.flink2check.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.prajwaljadhav01.flink2check.model.Finding;
import io.github.prajwaljadhav01.flink2check.model.Report;
import io.github.prajwaljadhav01.flink2check.model.Severity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Renders a {@link Report} as plain text, Markdown or JSON. */
public final class Reporters {

    private Reporters() { }

    public static String render(Report r, String format, boolean color) {
        return switch (format) {
            case "markdown", "md" -> markdown(r);
            case "json" -> json(r);
            default -> text(r, color);
        };
    }

    // ---------------------------------------------------------------- text

    static String text(Report r, boolean color) {
        StringBuilder sb = new StringBuilder();
        sb.append(bold("flink2-check", color)).append(" - target Flink ").append(r.target()).append('\n');
        r.facts().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        sb.append('\n');
        for (Severity s : Severity.values()) {
            List<Finding> fs = r.findings().stream().filter(f -> f.severity() == s).toList();
            if (fs.isEmpty()) {
                continue;
            }
            sb.append(paint(s, s + " (" + fs.size() + ")", color)).append('\n');
            Map<String, List<Finding>> byTitle = group(fs);
            for (Map.Entry<String, List<Finding>> e : byTitle.entrySet()) {
                List<Finding> items = e.getValue();
                boolean sameFix = sameFix(items);
                sb.append("  ").append(bold(e.getKey(), color)).append('\n');
                for (Finding f : items) {
                    sb.append("    ").append(f.where()).append("  ").append(f.detail()).append('\n');
                    if (!sameFix && !f.fix().isBlank()) {
                        sb.append("      fix: ").append(f.fix()).append('\n');
                    }
                }
                Finding first = items.get(0);
                if (sameFix && !first.fix().isBlank()) {
                    sb.append("    fix: ").append(first.fix()).append('\n');
                }
                if (!first.link().isBlank()) {
                    sb.append("    see: ").append(first.link()).append('\n');
                }
            }
            sb.append('\n');
        }
        sb.append(verdictLine(r, color)).append('\n');
        return sb.toString();
    }

    private static String verdictLine(Report r, boolean color) {
        String counts = r.count(Severity.ERROR) + " errors, " + r.count(Severity.WARNING) + " warnings, "
                + r.count(Severity.INFO) + " notes";
        String v = switch (r.verdict()) {
            case BLOCKED -> "BLOCKED: fix the errors before moving to Flink " + r.target();
            case NEEDS_WORK -> "NEEDS WORK: no hard blockers found, review the warnings";
            case READY -> "READY: nothing found that blocks Flink " + r.target();
        };
        Severity s = switch (r.verdict()) {
            case BLOCKED -> Severity.ERROR;
            case NEEDS_WORK -> Severity.WARNING;
            case READY -> Severity.INFO;
        };
        return paint(s, v, color) + " (" + counts + ")";
    }

    private static boolean sameFix(List<Finding> items) {
        return items.stream().map(Finding::fix).distinct().count() == 1;
    }

    private static Map<String, List<Finding>> group(List<Finding> fs) {
        return fs.stream().collect(Collectors.groupingBy(Finding::title, LinkedHashMap::new, Collectors.toList()));
    }

    private static String paint(Severity s, String text, boolean color) {
        if (!color) {
            return text;
        }
        String code = switch (s) {
            case ERROR -> "\u001b[1;31m";
            case WARNING -> "\u001b[1;33m";
            case INFO -> "\u001b[1;32m";
        };
        return code + text + "\u001b[0m";
    }

    private static String bold(String text, boolean color) {
        return color ? "\u001b[1m" + text + "\u001b[0m" : text;
    }

    // ---------------------------------------------------------------- markdown

    static String markdown(Report r) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Flink ").append(r.target()).append(" upgrade check\n\n");
        String badge = switch (r.verdict()) {
            case BLOCKED -> "**BLOCKED**: fix the errors below before upgrading.";
            case NEEDS_WORK -> "**NEEDS WORK**: no hard blockers, review the warnings.";
            case READY -> "**READY**: nothing found that blocks the upgrade.";
        };
        sb.append(badge).append(' ')
                .append(r.count(Severity.ERROR)).append(" errors, ")
                .append(r.count(Severity.WARNING)).append(" warnings, ")
                .append(r.count(Severity.INFO)).append(" notes.\n\n");
        if (!r.facts().isEmpty()) {
            sb.append("| Fact | Value |\n| --- | --- |\n");
            r.facts().forEach((k, v) -> sb.append("| ").append(k).append(" | `").append(v).append("` |\n"));
            sb.append('\n');
        }
        for (Severity s : Severity.values()) {
            List<Finding> fs = r.findings().stream().filter(f -> f.severity() == s).toList();
            if (fs.isEmpty()) {
                continue;
            }
            sb.append("## ").append(switch (s) {
                case ERROR -> "Errors";
                case WARNING -> "Warnings";
                case INFO -> "Notes";
            }).append(" (").append(fs.size()).append(")\n\n");
            for (Map.Entry<String, List<Finding>> e : group(fs).entrySet()) {
                Finding first = e.getValue().get(0);
                boolean sameFix = sameFix(e.getValue());
                sb.append("### ").append(e.getKey()).append("\n\n");
                sb.append(sameFix ? "| Where | What |\n| --- | --- |\n" : "| Where | What | Fix |\n| --- | --- | --- |\n");
                for (Finding f : e.getValue()) {
                    sb.append("| `").append(esc(f.where())).append("` | ").append(esc(f.detail()));
                    if (!sameFix) {
                        sb.append(" | ").append(esc(f.fix()));
                    }
                    sb.append(" |\n");
                }
                sb.append('\n');
                if (sameFix && !first.fix().isBlank()) {
                    sb.append("**Fix:** ").append(first.fix());
                    if (!first.link().isBlank()) {
                        sb.append(" ([docs](").append(first.link()).append("))");
                    }
                    sb.append("\n\n");
                }
            }
        }
        return sb.toString();
    }

    private static String esc(String s) {
        return s.replace("|", "\\|").replace("\n", " ");
    }

    // ---------------------------------------------------------------- json

    static String json(Report r) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("target", r.target());
        out.put("verdict", r.verdict().name());
        out.put("errors", r.count(Severity.ERROR));
        out.put("warnings", r.count(Severity.WARNING));
        out.put("notes", r.count(Severity.INFO));
        out.put("facts", r.facts());
        out.put("findings", r.findings());
        try {
            return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(out) + "\n";
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
