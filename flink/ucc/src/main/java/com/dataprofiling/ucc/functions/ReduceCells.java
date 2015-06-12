package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.primitives.Longs;
import org.apache.flink.util.Collector;

import com.dataprofiling.ucc.Candidate;
import com.dataprofiling.ucc.Cell;

/**
 * input cell, rowIndex
 * output columnIndex, list of rowIndex from duplicates
 * if a column has no duplicates, the list of rowIndex only includes on element
 * @author jpollak, pjung
 *
 */
public class ReduceCells implements GroupCombineFunction<Tuple2<Cell, long[]>, Tuple2<Candidate, long[]>>{
    private static final long serialVersionUID = 3593450793210883150L;

    @Override
    public void combine(Iterable<Tuple2<Cell, long[]>> in, Collector<Tuple2<Candidate, long[]>> out) throws Exception {
        Iterator<Tuple2<Cell, long[]>> cells = in.iterator();
        List<Long> rowIndices = new ArrayList<Long>();
        Tuple2<Cell, long[]> cell = null;
        while (cells.hasNext()) {
            cell = cells.next();
            rowIndices.add(cell.f1[0]);
        }
        // filter possible here? - possible to return noting?
        if (rowIndices.size() > 1) {
            // TODO: save size at beginng to much- later better?
            int size = rowIndices.size();
            rowIndices.add(0, (long) size);
            long[] outRowIndices = Longs.toArray(rowIndices);
            out.collect(new Tuple2<Candidate, long[]>(cell.f0.columnIndex, outRowIndices));
        }
    }

 

}
