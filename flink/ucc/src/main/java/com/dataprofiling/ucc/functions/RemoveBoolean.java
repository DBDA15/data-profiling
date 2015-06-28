package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class RemoveBoolean implements FlatMapFunction<Tuple2<Long, Boolean>, Long> {
    private static final long serialVersionUID = 2794239650598180846L;

    @Override
    public void flatMap(Tuple2<Long, Boolean> value, Collector<Long> out) throws Exception {
        out.collect(value.f0);
    }
}
