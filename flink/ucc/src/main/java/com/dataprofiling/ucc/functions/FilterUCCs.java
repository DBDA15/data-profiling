package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterUCCs implements FilterFunction<Tuple2<Long, Boolean>> {
    private static final long serialVersionUID = 3000811875946465408L;

    @Override
    public boolean filter(Tuple2<Long, Boolean> value) throws Exception {
        // TODO Auto-generated method stub
        return !value.f1;
    }

}
