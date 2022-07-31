package org.apache.flink.training.exercises.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.test.TestSink;


public class TestPipeline<IN,OUT> implements ExecutablePipeline<IN,OUT>{

    private final ExecutablePipeline<IN, OUT> somePipeline;

    public TestPipeline(ExecutablePipeline<IN,OUT> somePipeline){
        this.somePipeline = somePipeline;

    }

    // TODO，移除testing package
    @Override
    public JobExecutionResult execute(SourceFunction<IN> source, TestSink<OUT> sink) throws Exception {
        return somePipeline.execute(source,sink);
    }
}
