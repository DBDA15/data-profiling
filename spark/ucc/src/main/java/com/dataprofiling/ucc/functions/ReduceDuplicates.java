package com.dataprofiling.ucc.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.Function2;

public class ReduceDuplicates implements Function2<long[], long[], long[]> {
    private static final long serialVersionUID = 829715860662736678L;

    @Override
    public long[] call(long[] v1, long[] v2) throws Exception {
        return ArrayUtils.addAll(v1, v2);
    }
}
