package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CreateCells extends RichFlatMapFunction<String, Tuple2<long[], String>> {
    private static final long serialVersionUID = 1L;

    private final int offset;
    private long index = 0;
    
    private final String splitChar;

    private final Tuple2<long[], String> outputTuple = new Tuple2<long[], String>(new long[1], null);

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
    public void flatMap(String line, Collector<Tuple2<long[], String>> out) throws Exception {
        // TOFIX: gets whole file not only a line
        String[] fields = line.split(this.splitChar);
        int workerID = getRuntimeContext().getIndexOfThisSubtask() + 1;
        for (String field : fields) {
            this.outputTuple.f0[0] = offset * index + workerID;
            this.outputTuple.f1 = field;
            out.collect(this.outputTuple);
        }
        index++;
    }
}
