package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//采用side output方法进行split
public class RideCleansingExercise2 {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;
    private final SinkFunction<TaxiRide> sidesink;

    private static final OutputTag<TaxiRide> outsideNYC = new OutputTag<TaxiRide>("outsideNYC") {};

    /** Creates a job using the source and sinks provided. */
    public RideCleansingExercise2(
            SourceFunction<TaxiRide> source,
            SinkFunction<TaxiRide> sink,
            SinkFunction<TaxiRide> sidesink) {

        this.source = source;
        this.sink = sink;
        this.sidesink = sidesink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        RideCleansingExercise2 job =
                new RideCleansingExercise2(
                        new TaxiRideGenerator(),
                        new PrintSinkFunction<>(),
                        new PrintSinkFunction<>(true));

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {
        // set up streaming execution environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // attach a stream of TaxiRides
        DataStream<TaxiRide> rides = env.addSource(source);

        // split the stream
        SingleOutputStreamOperator<TaxiRide> splitResult = rides.process(new StreamSplitter());

        splitResult.addSink(sink).name("inside NYC");
        splitResult.getSideOutput(outsideNYC).addSink(sidesink).name("outside NYC");

        // run the pipeline and return the result
        return env.execute("Split with side output");
    }

    public static class StreamSplitter extends ProcessFunction<TaxiRide, TaxiRide> {

        @Override
        public void processElement(
                TaxiRide taxiRide,
                Context ctx,
                Collector<TaxiRide> out)
                throws Exception {

            if (GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat)) {

                out.collect(taxiRide);
            } else {
                ctx.output(outsideNYC, taxiRide);
            }
        }
    }
}
