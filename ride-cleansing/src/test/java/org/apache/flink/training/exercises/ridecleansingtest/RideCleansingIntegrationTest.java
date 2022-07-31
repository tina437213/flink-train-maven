package org.apache.flink.training.exercises.ridecleansingtest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.ridecleansing.RideCleansingExercise;
import org.apache.flink.training.exercises.test.ExecutablePipeline;
import org.apache.flink.training.exercises.test.ParallelTestSource;
import org.apache.flink.training.exercises.test.TestPipeline;

import org.apache.flink.training.exercises.test.TestSink;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class RideCleansingIntegrationTest extends RideCleansingTestBase{
    static final int PARALLELISM = 2;

    protected TestPipeline<TaxiRide, TaxiRide> rideCleansingPipeline() {
        ExecutablePipeline<TaxiRide, TaxiRide> exercise =
                (source, sink) -> (new RideCleansingExercise(source, sink)).execute();

        return new TestPipeline<>(exercise);
    }

    @Test
    public void testAMixtureOfLocations() throws Exception {

        TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
        TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
        TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
        TaxiRide atPennStation2 = testRide(-74.9947F, 40.750626F, -73.9947F, 40.750626F);
        TaxiRide atNorthPole = testRide(0, 90, 0, 90);


        ParallelTestSource<TaxiRide> source = new ParallelTestSource<>(toThePole, fromThePole, atPennStation, atNorthPole);
        TestSink<TaxiRide> sink = new TestSink<>();

        JobExecutionResult jobResult = rideCleansingPipeline().execute(source,sink);
        assertThat(sink.getResults(jobResult)).containsExactly(atPennStation);
        //assertThat(sink.getResults(jobResult)).containsExactlyElementsOf(Arrays.asList(atPennStation,atPennStation2));
    }
}
