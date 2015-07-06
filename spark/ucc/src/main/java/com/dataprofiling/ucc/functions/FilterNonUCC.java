package com.dataprofiling.ucc.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class FilterNonUCC implements Function<Tuple2<Long, Boolean>, Boolean> {
    private static final long serialVersionUID = 4753956668797689559L;

    @Override
    public Boolean call(Tuple2<Long, Boolean> v1) throws Exception {
        return v1._2;
    }

}
