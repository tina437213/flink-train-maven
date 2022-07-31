package org.apache.flink.training.exercises.common.testwt;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Test2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> data = env
                .fromElements("Flink", "DROP", "Forward", "IGNORE")
                .keyBy(x -> x);

        control
                .connect(data)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String controlValue, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(dataValue);
            }
        }
    }


}
