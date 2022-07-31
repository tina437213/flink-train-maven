package com.tina.flink.test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationFilterExp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.fromElements(1,2,3);

        source.filter(x -> x%2 ==1).print();

        env.execute();

    }
}
