package com.dataprofiling.ucc.functions;

import java.util.BitSet;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.dataprofiling.ucc.helper.Bits;


public class CreateCells extends RichFlatMapFunction<String, Tuple3<Long, String, long[]>> {
    private static final long serialVersionUID = 1L;

    private final int offset;
    private long index = 0;
    
    private final String splitChar;

    private final Tuple3<Long, String, long[]> outputTuple = new Tuple3<Long, String, long[]>(new Long(0), "", new long[1]);

    /**
     * Creates a new instance of this function.
     * @param splitChar is the CSV field separator
     */
    public CreateCells(char splitChar, int offset, long lineIndex) {
        this.splitChar = String.valueOf(splitChar);
        this.offset = offset;
        index = lineIndex;
    }

    @Override
    public void flatMap(String line, Collector<Tuple3<Long, String, long[]>> out) throws Exception {
        String[] fields = line.split(this.splitChar);
        int workerID = getRuntimeContext().getIndexOfThisSubtask() + 1;
        int column = 0;
        this.outputTuple.f2[0] = offset * index + workerID;
        for (String field : fields) {
            this.outputTuple.f0 = Bits.createLong(column, field.length());
            this.outputTuple.f1 = field;
            out.collect(this.outputTuple);
            column++;
        }
        index++;
    }
}
