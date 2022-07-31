package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

public class RideCleansingExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;

    /** Creates a job using the source and sink provided. */
    public RideCleansingExercise(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {

        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        RideCleansingExercise job = new RideCleansingExercise(new TaxiRideGenerator(),new PrintSinkFunction<>());
        job.execute();

    }


    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(source).filter(new NYCFilter()).addSink(sink);

        return env.execute("Taxi Ride Cleansing");


    }

    public static class NYCFilter implements FilterFunction<TaxiRide>{
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            throw new MissingSolutionException();
            /*return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);*/
        }

    }
}
