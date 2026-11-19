package com.example.vitals;

/** One heart-rate reading from a bedside monitor (synthetic data, no real patients). */
public class VitalReading {
    public String patientId;
    public long timestamp;
    public double heartRate;

    public VitalReading() { }

    public VitalReading(String patientId, long timestamp, double heartRate) {
        this.patientId = patientId;
        this.timestamp = timestamp;
        this.heartRate = heartRate;
    }

    public static VitalReading parse(String csv) {
        String[] p = csv.split(",");
        return new VitalReading(p[0], Long.parseLong(p[1]), Double.parseDouble(p[2]));
    }
}
