package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink){
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> rides = env.addSource(source);

        // TODO，自定义watermarkStrategy，处理迟到/无序事件，此处具体有什么用呢？
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((ride,t) -> ride.getEventTimeMillis());
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        return env.execute("Long Taxi Rides");
    }

    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new TaxiRideGenerator(),new PrintSinkFunction<>());
        job.execute();
    }

    public static class AlertFunction extends KeyedProcessFunction<Long,TaxiRide,Long>{
        private ValueState<TaxiRide> rideState;
        @Override
        public void open(Configuration config){
            ValueStateDescriptor<TaxiRide> rideStateDescriptor =
                    new ValueStateDescriptor<>("ride event", TaxiRide.class);
            rideState = getRuntimeContext().getState(rideStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide taxiRide, Context context, Collector<Long> out) throws Exception {
            TaxiRide firstRideEvent = rideState.value();

            if(firstRideEvent == null){
                //无论是开始事件和结束事件谁先到来，都记录到状态中
                rideState.update(taxiRide);

                // 如果是开始事件，生成一个计时器
                if(taxiRide.isStart){
                    context.timerService().registerEventTimeTimer(getTimerTime(taxiRide));
                }
            }else{
                if(taxiRide.isStart){
                    if (rideTooLong(taxiRide, firstRideEvent)) {
                        out.collect(taxiRide.rideId);
                    }
                }else{
                    //状态中存储了start事件，计时器仍在运行，除非它被触发了。此处删除计时器
                    context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

                    // 有可能没有触发计时器报警，再次判断下输出
                    if(rideTooLong(firstRideEvent,taxiRide)){
                        out.collect(taxiRide.rideId);
                    }
                }

                //因为两个事件都出现了，所以可以清空状态
                //当一个事件丢失时，本处理方法可能会导致内存泄漏
                // TODO，后面我们更多讨论
                rideState.clear();

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws IOException {
            // 超时行程，触发计时器
            out.collect(rideState.value().rideId);

            // 清空状态，防止再次触发。但是会引发一个新问题，当end事件到达时，会漏掉之前的开始状态
            rideState.clear();
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart) {
                return ride.eventTime.plusSeconds(120 * 60).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.eventTime, endEvent.eventTime)
                    .compareTo(Duration.ofHours(2))
                    > 0;
        }


    }



}
