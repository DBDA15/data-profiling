package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.primitives.Longs;
import org.apache.flink.util.Collector;

import com.dataprofiling.ucc.Candidate;

public class ReduceColumnIndices implements GroupReduceFunction<Tuple2<Candidate, long[]>, Tuple2<Candidate, long[]>> {
    private static final long serialVersionUID = 3591070793210883150L;

    @Override
    public void reduce(Iterable<Tuple2<Candidate, long[]>> in, Collector<Tuple2<Candidate, long[]>> out) throws Exception {
        Iterator<Tuple2<Candidate, long[]>> cells = in.iterator();
        List<Long> rowIndices = new ArrayList<Long>();
        Tuple2<Candidate, long[]> cell = null;
        while (cells.hasNext()) {
            cell = cells.next();
            for (int i = 0; i < cell.f1.length; i++) {
                rowIndices.add(cell.f1[i]);
            }
        }
        out.collect(new Tuple2<Candidate, long[]>(cell.f0, Longs.toArray(rowIndices)));
        
    }

}
