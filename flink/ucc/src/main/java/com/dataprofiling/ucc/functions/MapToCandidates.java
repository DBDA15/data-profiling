package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MapToCandidates implements FlatMapFunction<Tuple2<Long, long[]>, Long> {
    private static final long serialVersionUID = 3461643461937543872L;

    @Override
    public void flatMap(Tuple2<Long, long[]> in, Collector<Long> out) throws Exception {
        long[] tupleList = in.f1;
        for (int i = 0; i < tupleList.length - 1; i++) {
            for (int j = i + 1; j < tupleList.length; j++) {
                out.collect( tupleList[i] | tupleList[j]);
            }
        }       
    }
}
