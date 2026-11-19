package io.github.prajwaljadhav01.flink2check.model;

/**
 * One thing the scanner found.
 *
 * @param severity how much it blocks the upgrade
 * @param area     code, build, config or state
 * @param ruleId   stable id, e.g. {@code removed-class/source-function}
 * @param title    one-line summary
 * @param detail   what exactly was found
 * @param location file path (or savepoint operator) where it was found
 * @param line     1-based line number, or 0 when not applicable
 * @param fix      what to do about it
 * @param link     where to read more
 */
public record Finding(
        Severity severity,
        String area,
        String ruleId,
        String title,
        String detail,
        String location,
        int line,
        String fix,
        String link) {

    public String where() {
        return line > 0 ? location + ":" + line : location;
    }
}
