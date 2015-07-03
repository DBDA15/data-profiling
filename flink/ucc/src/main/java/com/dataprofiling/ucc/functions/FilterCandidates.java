package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FilterCandidates implements FilterFunction<Tuple2<Long, Boolean>> {
    private static final long serialVersionUID = -4869032300109425612L;

    @Override
    public boolean filter(Tuple2<Long, Boolean> value) throws Exception {
        return value.f1;
    }

}
