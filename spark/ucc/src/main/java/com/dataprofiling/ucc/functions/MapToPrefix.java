package com.dataprofiling.ucc.functions;

import java.util.BitSet;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MapToPrefix implements PairFunction<Long, Long, long[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Long, long[]> call(Long in) throws Exception {
        BitSet inB = Bits.convert(in);
        BitSet bitSet = (BitSet) inB.clone();
        int highestBit = bitSet.previousSetBit(bitSet
                .length());
        bitSet.clear(highestBit);
        long[] inArray= {in};
        return new Tuple2<Long, long[]>(Bits.convert(bitSet), inArray);

    }

}
