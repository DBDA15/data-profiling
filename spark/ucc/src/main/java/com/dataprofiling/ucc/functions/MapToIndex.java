package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MapToIndex implements Function<Tuple2<Long, long[]>, Long> {
    private static final long serialVersionUID = -2860162102354169961L;

    @Override
    public Long call(Tuple2<Long, long[]> v1) throws Exception {
        return v1._1;
    }

}
