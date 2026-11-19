package com.example.vitals;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/** Old-style source: SourceFunction and friends are gone in Flink 2.x. */
public class SyntheticVitalsSource extends RichParallelSourceFunction<VitalReading> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<VitalReading> ctx) throws Exception {
        Random random = new Random(getRuntimeContext().getIndexOfThisSubtask());
        while (running) {
            String patient = "P" + random.nextInt(50);
            ctx.collect(new VitalReading(patient, System.currentTimeMillis(), 50 + random.nextGaussian() * 20 + 30));
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
