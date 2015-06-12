package com.dataprofiling.ucc;

import com.dataprofiling.ucc.functions.CreateCells;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

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

        DataSource<String> lines = env.readTextFile(inputPath).name(
                "Load " + inputPath);

        DataSet<Tuple2<long[], String>> cells = lines.flatMap(
                new CreateCells(',', env.getParallelism(), 0)).name("Parse " + inputPath);

        
        collectAndPrintUccs(cells);
        // Trigger the job execution and measure the exeuction time.
        long startTime = System.currentTimeMillis();
        try {
            env.execute("UCC");
        } finally {
            RemoteCollectorImpl.shutdownAll();
        }
        long endTime = System.currentTimeMillis();
        System.out.format("Exection finished after %.3f s.\n",
                (endTime - startTime) / 1000d);
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
    
    private void collectAndPrintUccs(DataSet<Tuple2<long[], String>> cells) {
        RemoteCollectorImpl.collectLocal(cells, new RemoteCollectorConsumer<Tuple2<long[], String>>() {
            @Override
            public void collect(Tuple2<long[], String> cells) {
                for (long referencedAttributeIndex : cells.f0) {
                    System.out.format("%s < %s\n", referencedAttributeIndex, cells.f1);
                }
            }
        });
    }
}