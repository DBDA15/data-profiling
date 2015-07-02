package com.dataprofiling.ucc;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.dataprofiling.ucc.functions.FilterNonUniques;
import com.dataprofiling.ucc.functions.FilterUniques;
import com.dataprofiling.ucc.functions.GetCandidates;
import com.dataprofiling.ucc.functions.MapToCandidates;
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
                .name("Reduce cells to list of row indices").filter(new FilterFunction<Tuple3<Long, String, long[]>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(Tuple3<Long, String, long[]> v1) throws Exception {
                        return v1.f2.length != 1;
                    }
                }).name("filter single row indices").flatMap(new SkipCellValues())
                .name("Skip cell values, keep candidate and list of row indices")
                // 2, 1, 5, 3, 7, 8, 9 -> candidate has 2 "duplicates": 1 and 5
                // have same value, and 7, 8 and 9 have same value
                .groupBy(0).reduce(new ReduceDuplicates()).name("ReduceByCandidates to list of dublicate");

        // get current candidates from single column PLIs
        DataSet<Long> currentCandidates = singleColumnPLIs.flatMap(new GetCandidates()).name(
                "Map from PLI to candidate only");

        // create min Ucc
        Set<Long> minUCC = new HashSet<Long>();
        List<Long> currentCandidateList = currentCandidates.collect();
        int n = lines.first(1).collect().get(0).split(",").length;
        for (int i = 0; i < n; i++) {
            Long column = Bits.createLong(i, n);
            if (!currentCandidateList.contains(column)) {
                minUCC.add(column);
            }
        }

        while (currentCandidates.count() > 1) {
            // generate candidates using APRIORI algorithm
            DataSet<Long> candidates = currentCandidates.flatMap(new MapToPrefix())
                    .name("Candidate generation I: Map from candidate to pair: Prefix, Candidates").groupBy(0)
                    .reduce(new ReduceToPair()).name("candidate generation II").flatMap(new MapToCandidates())
                    .name("Candidate generation III: Map from (prefix, candidates) to candidates");

            // TODO: to subset check here instead before adding to minUccs

            // distribute single column PLIs using broadcast, alternative: copy
            // using flatmap and then partioning
            // create new PLIs on nodes
            candidates.rebalance();
            DataSet<Tuple2<Long, Boolean>> nextLevelPLI = candidates.flatMap(new MapToInsectedPLIs())
                    .name("intersected").withBroadcastSet(singleColumnPLIs, "singleColumnPLIs");

            // filter uniques
            List<Long> newUniques = nextLevelPLI.filter(new FilterUniques()).name("filter uniques").flatMap(new RemoveBoolean()).collect();
            for (Long newUnique : newUniques) {
                if (!isSubsetUnique(newUnique, minUCC)){
                    minUCC.add(newUnique);
                }
            }

            currentCandidates = nextLevelPLI.filter(new FilterNonUniques()).name("filter non uniques").flatMap(new RemoveBoolean());
        }

        print(minUCC);

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
    
    private void collectAndPrintUccs(DataSet<Tuple2<Long, Boolean>> a) {
        RemoteCollectorImpl.collectLocal(a, new RemoteCollectorConsumer<Tuple2<Long, Boolean>>() {
            @Override
            public void collect(Tuple2<Long, Boolean> cells) {
                System.out.format("%s < %s\n", cells.f0, cells.f1);
            }
        });
    }
}