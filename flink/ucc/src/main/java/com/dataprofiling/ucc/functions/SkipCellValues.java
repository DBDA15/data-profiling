package com.dataprofiling.ucc.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class SkipCellValues implements MapFunction<Tuple3<Long, String, long[]>, Tuple2<Long, long[]>> {
    private static final long serialVersionUID = 315443475189842114L;

    @Override
    public Tuple2<Long, long[]> map(Tuple3<Long, String, long[]> in) throws Exception {
        if (in.f2.length <= 1) {
            return new Tuple2<Long, long[]>(in.f0, new long[0]);
        }
        long[] size0 = { in.f2.length };
        long[] dupl0 = ArrayUtils.addAll(size0, in.f2);
        return new Tuple2<Long, long[]>(in.f0, dupl0);
    }

}
