package io.github.prajwaljadhav01.flink2check.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** All findings of one run, plus facts the scanners learned along the way. */
public final class Report {

    public enum Verdict { READY, NEEDS_WORK, BLOCKED }

    private final List<Finding> findings = new ArrayList<>();
    private final Set<String> dedup = new LinkedHashSet<>();
    private final Map<String, String> facts = new LinkedHashMap<>();
    private final String target;

    public Report(String target) {
        this.target = target;
    }

    public void add(Finding f) {
        String key = f.ruleId() + "|" + f.location() + "|" + f.line() + "|" + f.detail();
        if (dedup.add(key)) {
            findings.add(f);
        }
    }

    public void fact(String name, String value) {
        facts.put(name, value);
    }

    public Map<String, String> facts() {
        return facts;
    }

    public String target() {
        return target;
    }

    public List<Finding> findings() {
        List<Finding> sorted = new ArrayList<>(findings);
        sorted.sort(Comparator.comparing(Finding::severity)
                .thenComparing(Finding::area)
                .thenComparing(Finding::location)
                .thenComparingInt(Finding::line));
        return sorted;
    }

    public long count(Severity s) {
        return findings.stream().filter(f -> f.severity() == s).count();
    }

    public Verdict verdict() {
        if (count(Severity.ERROR) > 0) {
            return Verdict.BLOCKED;
        }
        if (count(Severity.WARNING) > 0) {
            return Verdict.NEEDS_WORK;
        }
        return Verdict.READY;
    }
}
