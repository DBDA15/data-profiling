package com.dataprofiling.ucc.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class CreateLines implements FlatMapFunction<String, String> {
    private static final long serialVersionUID = 1L;

    private final String splitChar;

    /**
     * Creates a new instance of this function.
     * @param splitChar is the CSV field separator
     */
    public CreateLines(String splitChar) {
        this.splitChar = String.valueOf(splitChar);
    }

    @Override
    public void flatMap(String line, Collector<String> out) throws Exception {
        String[] fields = line.split(this.splitChar);
        for (String field : fields) {
            out.collect(field);
        }
    }
}
