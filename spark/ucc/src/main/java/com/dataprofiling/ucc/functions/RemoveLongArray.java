package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class RemoveLongArray implements Function<Tuple2<Long, long[]>, Long> {
    private static final long serialVersionUID = 6406583775777660160L;

    @Override
    public Long call(Tuple2<Long, long[]> v1) throws Exception {
        return v1._1;
    }

}
