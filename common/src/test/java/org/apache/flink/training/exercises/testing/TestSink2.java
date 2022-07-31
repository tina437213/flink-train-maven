package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

public class TestSink2<OUT> extends RichSinkFunction<OUT> {

    private final String name;

    public TestSink2(String name) {
        this.name = name;
    }

    public TestSink2() {
        this("results");
    }

    @Override
    public void open(Configuration parameters) {
        getRuntimeContext().addAccumulator(name, new ListAccumulator<OUT>());
    }

    @Override
    public void invoke(OUT value, Context context) {
        getRuntimeContext().getAccumulator(name).add(value);
    }

    public List<OUT> getResults(JobExecutionResult jobResult) {
        return jobResult.getAccumulatorResult(name);
    }
}
