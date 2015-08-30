package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MapToCombined implements MapFunction<Tuple2<Long, long[]>, Tuple2<Long, Boolean>> {
    private static final long serialVersionUID = 7253043460589054833L;

    @Override
    public Tuple2<Long, Boolean> map(Tuple2<Long, long[]> value) throws Exception {
        // second parameter of return type is flag whether the Long represents a candidate or a UCC
        // it is true is it represents a candidate
        return new Tuple2<Long, Boolean>(value.f0, value.f1.length >= 1);
    }

}
