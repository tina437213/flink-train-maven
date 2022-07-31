package org.apache.flink.training.exercises.testing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface ExecutablePipeline<IN, OUT> {
    JobExecutionResult execute(SourceFunction<IN> source, TestSink2<OUT> sink) throws Exception;
}
