package com.dataprofiling.ucc.functions;

import java.util.BitSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.dataprofiling.ucc.helper.Bits;

public class MapToPrefix implements FlatMapFunction<Long, Tuple2<Long, long[]>> {
    private static final long serialVersionUID = 3461648361937543872L;

    @Override
    public void flatMap(Long in, Collector<Tuple2<Long, long[]>> out) throws Exception {
        BitSet inB = Bits.convert(in);
        BitSet bitSet = (BitSet) inB.clone();
        int highestBit = bitSet.previousSetBit(bitSet
                .length());
        bitSet.clear(highestBit);
        long[] inArray= {in};
        out.collect(new Tuple2<Long, long[]>(Bits.convert(bitSet), inArray));
    }
}
