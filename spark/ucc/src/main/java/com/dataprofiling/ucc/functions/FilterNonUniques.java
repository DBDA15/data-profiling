package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class FilterNonUniques implements Function<Tuple2<Long, long[]>, Boolean> {
    private static final long serialVersionUID = 1L;

    @Override
    public Boolean call(Tuple2<Long, long[]> v1) throws Exception {
        return v1._2.length > 0;
    }

}
