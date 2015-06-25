package com.dataprofiling.ucc.functions;

import java.util.BitSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.dataprofiling.ucc.Candidate;
import com.dataprofiling.ucc.Cell;

public class SkipCellValues extends RichFlatMapFunction<Tuple3<Long, String, long[]>, Tuple2<Long, long[]>>{
    private static final long serialVersionUID = 315443475189842114L;
    public SkipCellValues(){}
    
    @Override
    public void flatMap(Tuple3<Long, String, long[]> in, Collector<Tuple2<Long, long[]>> out) throws Exception {
        long[] size0 = {in.f2.length};
        long[] dupl0 =  ArrayUtils.addAll(size0, in.f2);
        out.collect(new Tuple2<Long, long[]>(in.f0, dupl0));
    }
}
