package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

public class RideCleansingExercise3 {

    private SourceFunction<TaxiRide> source;
    private SinkFunction<TaxiRide> sink;

    public RideCleansingExercise3(SourceFunction<TaxiRide> source,SinkFunction<TaxiRide> sink){
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> taxiRide = env.addSource(source);
        taxiRide.filter(new NYCFilter()).addSink(sink);
        return env.execute();
    }

    public static class NYCFilter implements FilterFunction<TaxiRide>{
        public boolean filter(TaxiRide taxiRide){
            return GeoUtils.isInNYC(taxiRide.startLon,taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon,taxiRide.endLat);
        }

    }



}
