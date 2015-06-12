package com.dataprofiling.ucc;

import java.util.BitSet;
import java.util.List;

import com.dataprofiling.ucc.functions.CreateCells;
import com.dataprofiling.ucc.functions.CreateLines;
import com.dataprofiling.ucc.functions.ReduceCells;
import com.dataprofiling.ucc.functions.ReduceColumnIndices;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

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
        DataSet<Tuple2<Cell, long[]>> cells = lines.flatMap(new CreateCells(',', env.getParallelism(), 0)).name(
                "Split line, Parse " + inputPath);

        // Create PLIs for single columns // TODO: no column Index
        DataSet<Tuple2<Cell, long[]>> b = cells.groupBy(0).getDataSet();
        System.out.println("CELLS " + b.collect());
        
        DataSet<Tuple2<Candidate, long[]>> a = cells.combineGroup(new ReduceCells());
        //.reduceGroup(new ReduceCells()).name("Reduce cells");
       // DataSet<Tuple2<Candidate, long[]>> singleColumnPLIs = a.groupBy(0).reduceGroup(new ReduceColumnIndices()).name("Reduce column indices");
        
        //collectAndPrintUccs2(cells);
        collectAndPrintUccs(a);
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

    private void collectAndPrintUccs(DataSet<Tuple2<Candidate, long[]>> a) {
        RemoteCollectorImpl.collectLocal(a, new RemoteCollectorConsumer<Tuple2<Candidate, long[]>>() {
            @Override
            public void collect(Tuple2<Candidate, long[]> cells) {
                String pli = "[";
                for (long referencedAttributeIndex : cells.f1) {
                    pli += referencedAttributeIndex +", ";
                }
                System.out.format("%s < %s\n", cells.f0, pli);
            }
        });
    }
    
    private void collectAndPrintUccs2(DataSet<Tuple2<Cell, long[]>> cells) {
        RemoteCollectorImpl.collectLocal(cells, new RemoteCollectorConsumer<Tuple2<Cell, long[]>>() {
            @Override
            public void collect(Tuple2<Cell, long[]> cells) {
                    System.out.format("%s < %s\n", cells.f0.toString(), cells.f1[0]);
            }
        });
    }
}