package com.example.vitals;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/** Nightly batch job on the DataSet API, which Flink 2.x removed. */
public class DailySummaryBatchJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> counts = env.readTextFile(args[0])
                .map(line -> Tuple2.of(line.split(",")[0], 1))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT))
                .groupBy(0)
                .sum(1);
        counts.writeAsCsv(args[1]);
        env.execute("daily-summary");
    }
}
