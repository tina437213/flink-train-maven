package org.apache.flink.training.exercises.test;

import org.apache.flink.api.common.functions.FilterFunction;

//create by tina, only test exercise, not solution
public class TestFilterFunction<T> implements FilterFunction<T> {

    private final FilterFunction<T> someFilter;

    public TestFilterFunction(FilterFunction<T> someFilter){
        this.someFilter = someFilter;

    }
    @Override
    public boolean filter(T value) throws Exception {
        return someFilter.filter(value);
    }

}
