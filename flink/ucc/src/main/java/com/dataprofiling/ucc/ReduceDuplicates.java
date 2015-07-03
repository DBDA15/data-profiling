package com.dataprofiling.ucc;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ReduceDuplicates implements ReduceFunction<Tuple2<Long, long[]>> {
    private static final long serialVersionUID = 8603222809950749067L;

    @Override
    public Tuple2<Long, long[]> reduce(Tuple2<Long, long[]> v0, Tuple2<Long, long[]> v1) throws Exception {
        long[] rows = ArrayUtils.addAll(v0.f1, v1.f1);
        return new Tuple2<Long, long[]>(v0.f0, rows);
    }

}
