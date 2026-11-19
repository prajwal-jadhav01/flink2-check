package io.github.prajwaljadhav01.flink2check.model;

/** How much a finding blocks the upgrade. */
public enum Severity {
    /** Will not compile, will not start, or will not restore state on Flink 2.x. */
    ERROR,
    /** Probably breaks or changes behaviour; needs a human look. */
    WARNING,
    /** Worth knowing; no action required. */
    INFO
}
