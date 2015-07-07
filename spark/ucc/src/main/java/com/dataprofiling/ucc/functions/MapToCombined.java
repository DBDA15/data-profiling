package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MapToCombined implements Function<Tuple2<Long, long[]>, Tuple2<Long, Boolean>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Long, Boolean> call(Tuple2<Long, long[]> value) throws Exception {
        // second parameter of return type is flag whether the Long represents a candidate or a UCC
        // it is true is it represents a candidate
        return new Tuple2<Long, Boolean>(value._1, value._2.length >= 1);
    }

}
