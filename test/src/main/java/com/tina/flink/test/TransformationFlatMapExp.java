package com.tina.flink.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformationFlatMapExp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.fromElements(1,2,3);

        // 必须声明 collector 类型
        source.flatMap((Integer number, Collector<String> out) -> {
                    StringBuilder builder = new StringBuilder();
                    for(int i = 0; i < number; i++) {
                        builder.append("a");
                        out.collect(builder.toString());
                    }
                })
                //显式提供类型信息
                .returns(Types.STRING)
                // 打印 "a", "a", "aa", "a", "aa", "aaa"
                .print();

        env.execute();
    }
}
