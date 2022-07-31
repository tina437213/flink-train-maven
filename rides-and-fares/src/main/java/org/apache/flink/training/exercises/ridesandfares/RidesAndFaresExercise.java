package org.apache.flink.training.exercises.ridesandfares;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.RideAndFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class RidesAndFaresExercise {
    private final SourceFunction<TaxiRide> ride;
    private final SourceFunction<TaxiFare> fare;
    private final SinkFunction<RideAndFare> sink;

    public RidesAndFaresExercise(
            SourceFunction<TaxiRide> ride,
            SourceFunction<TaxiFare> fare,
            SinkFunction<RideAndFare> sink){
        this.ride = ride;
        this.fare = fare;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> rides = env.addSource(ride).filter(ride->ride.isStart).keyBy(ride -> ride.rideId);
        DataStream<TaxiFare> fares = env.addSource(fare).keyBy(fare -> fare.rideId);

        rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(sink);
        return env.execute();
    }

    public static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide,TaxiFare,RideAndFare> {

        ValueState<TaxiRide> rideVs;
        ValueState<TaxiFare> fareVs;

        @Override
        public void open(Configuration config){
            ValueStateDescriptor rideDes = new ValueStateDescriptor<TaxiRide>("saved ride",TaxiRide.class);
            ValueStateDescriptor fareDes = new ValueStateDescriptor<TaxiFare>("saved fare",TaxiFare.class);

            rideVs = getRuntimeContext().getState(rideDes);
            fareVs = getRuntimeContext().getState(fareDes);
        }

        /**
         * flatMap函数返回类型是void，实际上输出类型都在Collector中体现
         * @param ride
         * @param out
         * @throws IOException
         */
        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws IOException {
            rideVs.update(ride);
            if(fareVs.value() != null){
                out.collect(new RideAndFare(ride,fareVs.value()));
            }
        }

        /**
         * flatMap函数返回类型是void，实际上输出类型都在Collector中体现
         * @param fare
         * @param out
         * @throws IOException
         */

        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws IOException {
            fareVs.update(fare);
            if(rideVs.value() != null){
                out.collect(new RideAndFare(rideVs.value(),fare));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        RidesAndFaresExercise job = new RidesAndFaresExercise(
                new TaxiRideGenerator(), new TaxiFareGenerator(),new PrintSinkFunction<>());
        job.execute();
    }

}
