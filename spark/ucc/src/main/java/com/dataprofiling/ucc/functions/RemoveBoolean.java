package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class RemoveBoolean implements Function<Tuple2<Long, Boolean>, Long> {
    private static final long serialVersionUID = 5457370190935967580L;

    @Override
    public Long call(Tuple2<Long, Boolean> v1) throws Exception {
        return v1._1;
    }

}
