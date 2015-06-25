package com.dataprofiling.ucc.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.dataprofiling.ucc.Cell;

/**
 * input cell, rowIndex
 * output columnIndex, list of rowIndex from duplicates
 * if a column has no duplicates, the list of rowIndex only includes on element
 * @author jpollak, pjung
 *
 */
public class ReduceCells implements ReduceFunction<Tuple3<Long, String, long[]>>{
    private static final long serialVersionUID = 3593450793210883150L;

    @Override
    public Tuple3<Long, String, long[]> reduce(Tuple3<Long, String, long[]> v1, Tuple3<Long, String, long[]> v2) throws Exception {
        long[] rows = ArrayUtils.addAll(v1.f2, v2.f2);
        return new Tuple3<Long, String, long[]>(v1.f0, v1.f1, rows);
    }

 

}
