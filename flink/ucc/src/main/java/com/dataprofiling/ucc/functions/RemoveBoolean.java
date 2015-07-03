package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class RemoveBoolean implements MapFunction<Tuple2<Long, Boolean>, Long> {
    private static final long serialVersionUID = 2794239650598180846L;

    @Override
    public Long map(Tuple2<Long, Boolean> value) throws Exception {
        return value.f0;
    }
}
