package org.apache.flink.training.exercises.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.testing.TestSink2;

public interface ExecutablePipeline<IN, OUT> {
    JobExecutionResult execute(SourceFunction<IN> source, TestSink<OUT> sink) throws Exception;
}
