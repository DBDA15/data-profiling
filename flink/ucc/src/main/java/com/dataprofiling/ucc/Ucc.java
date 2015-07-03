package com.dataprofiling.ucc;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.dataprofiling.ucc.functions.CreateCells;
import com.dataprofiling.ucc.functions.CreateLines;
import com.dataprofiling.ucc.functions.FilterCandidates;
import com.dataprofiling.ucc.functions.FilterUCCs;
import com.dataprofiling.ucc.functions.MapToCandidates;
import com.dataprofiling.ucc.functions.MapToCombined;
import com.dataprofiling.ucc.functions.MapToInsectedPLIs;
import com.dataprofiling.ucc.functions.MapToPrefix;
import com.dataprofiling.ucc.functions.ReduceCells;
import com.dataprofiling.ucc.functions.ReduceToPair;
import com.dataprofiling.ucc.functions.RemoveBoolean;
import com.dataprofiling.ucc.functions.SkipCellValues;
import com.dataprofiling.ucc.helper.Bits;

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
        long startTime = System.currentTimeMillis();
        // Load the execution environment.
        final ExecutionEnvironment env = createExecutionEnvironment();

        // Read and parse the input file.
        String inputPath = this.parameters.inputFile;
        DataSource<String> file = env.readTextFile(inputPath).name("Load " + inputPath);
        DataSet<String> lines = file.flatMap(new CreateLines("\r")).name("split file");
        DataSet<Tuple3<Long, String, long[]>> cells = lines.flatMap(new CreateCells(',', env.getParallelism(), 0))
                .name("Split line, Parse " + inputPath);

        // Create position list indices for single columns
        DataSet<Tuple2<Long, long[]>> singleColumnPLIs = cells.groupBy(0, 1).reduce(new ReduceCells())
                .name("Reduce cells to list of row indices")
                .map(new SkipCellValues())
                .name("Skip cell values, keep candidate and list of row indices")
                // 2, 1, 5, 3, 7, 8, 9 -> candidate has 2 "duplicates": 1 and 5
                // have same value, and 7, 8 and 9 have same value
                .groupBy(0).reduce(new ReduceDuplicates()).name("ReduceByCandidates to list of dublicate");

        // combine uccs and candidates to one dataset with boolean flag, which is true when column is candidate
        DataSet<Tuple2<Long, Boolean>> combined = singleColumnPLIs.map(new MapToCombined()).name("map to combined");
        IterativeDataSet<Tuple2<Long, Boolean>> inital = combined.iterate(10);

        // generate candidates using APRIORI algorithm
        DataSet<Long> candidates = inital.filter(new FilterCandidates()).map(new RemoveBoolean()).flatMap(new MapToPrefix())
                .name("Candidate generation I: Map from candidate to pair: Prefix, Candidates").groupBy(0)
                .reduce(new ReduceToPair()).name("candidate generation II").flatMap(new MapToCandidates())
                .name("Candidate generation III: Map from (prefix, candidates) to candidates");

        // TODO: to subset check here instead before adding to minUccs

        // distribute singleColumnPlis using broadcast (OR using flatmap and then partioning)
        // create new PLIs on nodes
        candidates = candidates.rebalance();
        DataSet<Tuple2<Long, Boolean>> nextLevelPLI = candidates.map(new MapToInsectedPLIs()).name("intersected")
                .withBroadcastSet(singleColumnPLIs, "singleColumnPLIs");

        // nextLevelPLI is combination of next level candidates and new minUccs therefore only have to add old minUccs
        DataSet<Tuple2<Long, Boolean>> iteration = nextLevelPLI.union(inital.filter(new FilterUCCs()));
        DataSet<Tuple2<Long, Boolean>> result = inital.closeWith(iteration);

        List<Long> uccs = result.filter(new FilterUCCs()).map(new RemoveBoolean()).collect();
        long endTime = System.currentTimeMillis();
        print(uccs);
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

    /**
     * Check if any of the minimal uniques is completely contained in the column
     * combination. If so, a subset is already unique.
     * 
     * TODO: inefficient due to comparisons to all minimal uniques and due to
     * convertion to BitSet
     * 
     * @param columnCombination
     * @param minUCC
     * @return true if columnCombination contains unique subset, false otherwise
     */
    private static boolean isSubsetUnique(Long columnCombinationL, Collection<Long> minUCC) {
        BitSet columnCombination = Bits.convert(columnCombinationL);
        for (Long bSetL : minUCC) {
            BitSet bSet = Bits.convert(bSetL);
            if (bSet.cardinality() > columnCombination.cardinality()) {
                continue;
            }
            BitSet copy = (BitSet) bSet.clone();
            copy.and(columnCombination);

            if (copy.cardinality() == bSet.cardinality()) {
                return true;
            }
        }
        return false;
    }

    private void print(Collection<Long> minUCCs) {
        for (Long ucc : minUCCs) {
            System.out.print(Bits.convert(ucc));
        }
        System.out.println();
    }
}