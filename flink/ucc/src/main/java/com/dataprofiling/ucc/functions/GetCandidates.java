package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class GetCandidates implements FlatMapFunction<Tuple2<Long, long[]>, Long> {
    private static final long serialVersionUID = -476505306939722749L;

    @Override
    public void flatMap(Tuple2<Long, long[]> in, Collector<Long> out) throws Exception {
        out.collect(in.f0);
    }

}
