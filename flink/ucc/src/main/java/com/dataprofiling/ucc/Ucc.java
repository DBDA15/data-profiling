package com.dataprofiling.ucc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.dataprofiling.ucc.functions.CreateCells;
import com.dataprofiling.ucc.functions.CreateLines;
import com.dataprofiling.ucc.functions.ReduceCells;
import com.dataprofiling.ucc.functions.SkipCellValues;

/**
 * Distributed UCC Discovery on Flink.
 * 
 * @author pjung, jpollak
 *
 */
public class Ucc {
    /** Stores execution parameters of this job. */
    private final Parameters parameters;

    public static void main(String[] args) throws Exception {
        Ucc ucc = new Ucc(args);
        ucc.run();
    }

    public Ucc(String[] args) {
        this.parameters = Parameters.parse(args);
    }

    private void run() throws Exception {
        // Load the execution environment.
        final ExecutionEnvironment env = createExecutionEnvironment();

        // Read and parse the input file.
        String inputPath = this.parameters.inputFile;
        DataSource<String> file = env.readTextFile(inputPath).name("Load " + inputPath);
        DataSet<String> lines = file.flatMap(new CreateLines("\r")).name("split file");
        DataSet<Tuple3<Long, String, long[]>> cells = lines.flatMap(new CreateCells(',', env.getParallelism(), 0)).name(
                "Split line, Parse " + inputPath);

        
        DataSet<Tuple3<Long, String, long[]>> a = cells.groupBy(0, 1).reduce(new ReduceCells()).name("Reduce cells to list of row indices")
                .filter(new FilterFunction<Tuple3<Long, String, long[]>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public boolean filter(Tuple3<Long, String, long[]> v1) throws Exception {
                        return v1.f2.length != 1;
                    }
                }).name("filter single row indices");
        
        DataSet<Tuple2<Long, long[]>> c = a.flatMap(new SkipCellValues()).name("Skip cell values, keep candidate and list of row indices");
        // 2, 1, 5, 3, 7, 8, 9 -> candidate has 2 "duplicates": 1 and 5 have same value, and 7, 8 and 9 have same value
        DataSet<Tuple2<Long, long[]>> d = c.groupBy(0).reduce(new ReduceDuplicates()).name("ReduceByCandidates to list of dublicate");
        // DataSet<Tuple2<Candidate, long[]>> singleColumnPLIs = a.groupBy(0).reduceGroup(new ReduceColumnIndices()).name("Reduce column indices");
        
        collectAndPrintUccs(d);
        
        
        // candidaten generien auf master
        
        
        // candidaten auf nodes verteilen
        // nodes checken minimale
        
        // master collect
        
        
        
        // Trigger the job execution and measure the exeuction time.
        long startTime = System.currentTimeMillis();
        try {
            env.execute("UCC");
        } finally {
            RemoteCollectorImpl.shutdownAll();
        }
        long endTime = System.currentTimeMillis();
        System.out.format("Exection finished after %.3f s.\n", (endTime - startTime) / 1000d);
    }

    /**
     * Creates a execution environment as specified by the parameters.
     */
    @SuppressWarnings("deprecation")
    private ExecutionEnvironment createExecutionEnvironment() {
        ExecutionEnvironment executionEnvironment;

        if (this.parameters.executor != null) {
            // If a remote executor is explicitly specified, connect.
            final String[] hostAndPort = this.parameters.executor.split(":");
            final String host = hostAndPort[0];
            final int port = Integer.parseInt(hostAndPort[1]);
            if (this.parameters.jars == null || this.parameters.jars.isEmpty()) {
                throw new IllegalStateException("No jars specified to be deployed for remote execution.");
            }
            final String[] jars = new String[this.parameters.jars.size()];
            this.parameters.jars.toArray(jars);
            executionEnvironment = ExecutionEnvironment.createRemoteEnvironment(host, port, jars);

        } else {
            // Otherwise, create a default exection environment.
            executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        }

        // Set the default parallelism explicitly, if requested.
        if (this.parameters.parallelism != -1) {
            executionEnvironment.setDegreeOfParallelism(this.parameters.parallelism);
        }

        return executionEnvironment;
    }

    private void collectAndPrintUccs(DataSet<Tuple2<Long, long[]>> a) {
        RemoteCollectorImpl.collectLocal(a, new RemoteCollectorConsumer<Tuple2<Long, long[]>>() {
            @Override
            public void collect(Tuple2<Long, long[]> cells) {
                String pli = "[";
                for (long referencedAttributeIndex : cells.f1) {
                    pli += referencedAttributeIndex +", ";
                }
                System.out.format("%s < %s\n", cells.f0, pli);
            }
        });
    }
}