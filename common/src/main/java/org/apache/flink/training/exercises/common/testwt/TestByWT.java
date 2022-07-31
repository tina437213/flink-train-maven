package org.apache.flink.training.exercises.common.testwt;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.sources.ReadLineSource;
import org.apache.flink.util.Collector;
import org.w3c.dom.html.HTMLImageElement;


public class TestByWT {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> stream = env.addSource(new ReadLineSource("C:\\project\\java\\data\\source.txt"));
        DataStream<Event> ss = stream.flatMap(
                new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String s, Collector<Event> collector) throws Exception {
                        String[] split = s.split(",");
                        if ("pv".equals(split[3])) {
                            Event event = new Event(split[0],Long.parseLong(split[4]));
                            collector.collect(event);
                        }
                    }
                }
        );

        /*ss.keyBy(e -> e.userid)
                .flatMap(new Deduplicate())
                .print();*/

        ss.keyBy(e -> e.userid)
                .filter(new Deduplicate())
                .print();


        env.execute();
    }

    public static class Deduplicate extends RichFilterFunction<Event> {
        ValueState<Boolean> seen;
        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            seen = getRuntimeContext().getState(desc);
        }

        @Override
        public boolean filter(Event event) throws Exception {
            if (seen.value() == null) {
                seen.update(true);
                return true;
            }
            return false;
        }
    }

    /*public static class Deduplicate extends RichFlatMapFunction<Event, Event> {
        ValueState<Boolean> seen;
        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            seen = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            if (seen.value() == null) {
                out.collect(event);
                seen.update(true);
            }
        }
    }*/



    private static class Event {
        public  String userid;
        public  long timestamp;
        public Event(){

        }
        public Event(String userid, long timestamp){
            this.userid = userid;
            this.timestamp = timestamp;
        }

        public String toString(){
            return userid+"-"+ timestamp;
        }
    }



}
