package com.example.vitals;

import java.util.ArrayList;
import java.util.List;

/**
 * Kept in keyed state. The List field makes Flink 1.x serialise it with Kryo,
 * which Flink 2.x cannot restore.
 */
public class RecentReadings {
    public String patientId;
    public List<Double> lastHeartRates = new ArrayList<>();
}
