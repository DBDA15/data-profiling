package com.dataprofiling.ucc.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ReduceToPair implements ReduceFunction<Tuple2<Long, long[]>> {
    private static final long serialVersionUID = 922362701807114204L;

    @Override
    public Tuple2<Long, long[]> reduce(Tuple2<Long, long[]> v0, Tuple2<Long, long[]> v1) throws Exception {
        long[] rows = ArrayUtils.addAll(v0.f1, v1.f1);
        return new Tuple2<Long, long[]>(v0.f0, rows);
    }

}
