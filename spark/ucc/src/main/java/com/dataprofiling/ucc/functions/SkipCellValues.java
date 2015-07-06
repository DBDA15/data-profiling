package com.dataprofiling.ucc.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dataprofiling.ucc.Cell;

public class SkipCellValues implements PairFunction<Tuple2<Cell, long[]>, Long, long[]> {
    private static final long serialVersionUID = 135443475189842114L;

    @Override
    public Tuple2<Long, long[]> call(Tuple2<Cell, long[]> in) throws Exception {
        if (in._2.length <= 1) {
            return new Tuple2<Long, long[]>(in._1.columnIndex, new long[0]);
        }
        long[] size0 = { in._2.length };
        long[] dupl0 = ArrayUtils.addAll(size0, in._2);
        return new Tuple2<Long, long[]>(in._1.columnIndex, dupl0);
    }
}
