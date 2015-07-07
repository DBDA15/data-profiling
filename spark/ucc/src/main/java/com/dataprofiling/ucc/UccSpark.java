package com.dataprofiling.ucc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.dataprofiling.ucc.functions.Bits;
import com.dataprofiling.ucc.functions.FilterNonUCC;
import com.dataprofiling.ucc.functions.FilterNonUniques;
import com.dataprofiling.ucc.functions.FilterUCC;
import com.dataprofiling.ucc.functions.FilterUniques;
import com.dataprofiling.ucc.functions.MapToCandidates;
import com.dataprofiling.ucc.functions.MapToIndex;
import com.dataprofiling.ucc.functions.MapToIntersectedPLI;
import com.dataprofiling.ucc.functions.MapToPrefix;
import com.dataprofiling.ucc.functions.ReduceDuplicates;
import com.dataprofiling.ucc.functions.RemoveBoolean;
import com.dataprofiling.ucc.functions.RemoveLongArray;
import com.dataprofiling.ucc.functions.SkipCellValues;

/**
 * Distributed UCC Discovery.
 * 
 * Possible optimizations: trie for keeping track of min uniques and subset
 * 
 * @author pjung, jpollak
 *
 */
public class UccSpark {
    /** Stores execution parameters of this job. */
    private final Parameters parameters;

    public UccSpark(String[] args) {
        this.parameters = Parameters.parse(args);
    }

    public static void main(String[] args) throws Exception {
        UccSpark ucc = new UccSpark(args);
        ucc.run();
    }

    private void run() {
        long start = System.currentTimeMillis();

        // by default, all lattice levels will be checked
        int levelsToCheck = this.parameters.levelsToCheck;
        final String inputFile = this.parameters.inputFile;
        String delimiter = this.parameters.delimiter;

        JavaSparkContext spark = createSparkContext();
        JavaRDD<String> file = spark.textFile(inputFile);

        // let slaves know what the column delimiter is
        Broadcast<String> bcDelimiter = spark.broadcast(delimiter);
        String localDelimiter = bcDelimiter.value();

        // get PLI for non unique columns
        JavaPairRDD<Cell, long[]> cellValues = createCellValues(file, localDelimiter);
        JavaPairRDD<Long, long[]> plisSingleColumns = createPLIs(cellValues);

        // save minUccs as RDD
        JavaRDD<Long> minUcc = plisSingleColumns.filter(new FilterUniques()).map(new RemoveLongArray());

        JavaPairRDD<Long, long[]> currentLevelPLIs = plisSingleColumns.filter(new FilterNonUniques()).cache();
        Broadcast<JavaRDD<Long>> broadcastMinUCC = spark.broadcast(minUcc);
        List<Long> localMinUcc = broadcastMinUCC.value().collect();

        boolean done = false;
        int currentLevel = 0;

        // save singleColumPLIs and broadcast it
        // TOFIX need to collect RDD before broadcast it?
        List<Tuple2<Long, long[]>> pliList = plisSingleColumns.collect();
        Broadcast<List<Tuple2<Long, long[]>>> broadcastSingleColPLI = spark.broadcast(pliList);
        List<Tuple2<Long, long[]>> localSingleColPLI = broadcastSingleColPLI.value();
        HashMap<Long, long[]> pliHashMap = new HashMap<Long, long[]>();
        for (int i = 0; i < localSingleColPLI.size(); i++) {
            Tuple2<Long, long[]> ele = localSingleColPLI.get(i);
            pliHashMap.put(ele._1, ele._2);
        }

        JavaRDD<Long> currentCandidates = currentLevelPLIs.map(new MapToIndex()).cache();

        while (!done && currentLevel < levelsToCheck) {
            long startLoop = System.currentTimeMillis();
            currentLevel++;

            // generate candidates
            JavaRDD<Long> candidates = generateCandidates(currentCandidates, localMinUcc);
            candidates.cache();
            currentCandidates.unpersist();
            
            JavaPairRDD<Long, Boolean> intersectedPLIs = generateNextLevelPLIs(pliHashMap, candidates,
                    spark.defaultParallelism());
            // intersectedPLIs.cache(); TOFIX: caching?

            JavaRDD<Long> additionalMinUcc = intersectedPLIs.filter(new FilterUCC()).map(new RemoveBoolean());

            minUcc.union(additionalMinUcc);

            JavaRDD<Long> nonUniqueCombinations = intersectedPLIs.filter(new FilterNonUCC()).map(new RemoveBoolean());

            if (nonUniqueCombinations.isEmpty()) {
                // abort processing once there are no new candidates to check
                done = true;
                break;
            }
            
            // prepare new round of candidate generation
            currentCandidates = nonUniqueCombinations;
            System.out.println("Finished another iteration: " + (System.currentTimeMillis() - startLoop) + "ms");
        }

        long end = System.currentTimeMillis();
        System.out.println("Runtime: " + (end - start) / 1000 + "s");
        System.out.print("Minimal Unique Column Combinations: ");
        print(minUcc.collect());
        spark.close();
    }

    private void print(List<Long> list) {
        for (Long ucc : list) {
            System.out.print(Bits.convert(ucc));
        }
        System.out.println();
    }

    private static JavaRDD<Long> generateCandidates(JavaRDD<Long> currentCandidates, final List<Long> localMinUcc) {
        return currentCandidates.mapToPair(new MapToPrefix()).groupByKey().flatMap(new MapToCandidates(localMinUcc));
    }

    private static JavaSparkContext createSparkContext() {
        SparkConf config = new SparkConf().setAppName("de.hpi.dbda.UccDiscovery");
        // config.set("spark.hadoop.validateOutputSpecs", "false");
        return new JavaSparkContext(config);
    }

    /**
     * This method reads the given file row by row and create a spark RDD of
     * Cells.
     * 
     * @param file
     *            spark file
     * @param bcDelimiter
     * @return
     */
    private static JavaPairRDD<Cell, long[]> createCellValues(JavaRDD<String> file, final String delimiter) {
        return file.flatMap(new FlatMapFunction<String, Tuple2<Cell, long[]>>() {
            private static final long serialVersionUID = 1L;
            int index = 0;
            
            @Override
            public Iterable<Tuple2<Cell, long[]>> call(String t) throws Exception {
                String[] strValues = t.split(delimiter);
                int N = strValues.length;
                List<Tuple2<Cell, long[]>> cells = new ArrayList<Tuple2<Cell, long[]>>();

                // like the birthday problem the probability of matching pairs:
                // (n(n-1)/2)/Long.MAX_VALUE
                // e.g. n= 10.000 p=5.4*10^-12, n=10^6 p=5.4 * 10^-8, n=10^9
                // p=0.054
                // most used table have less then 10^9 rows --> matching to rows
                // is unlikely
                long rowIndex = index;//(long) (Math.random() * Long.MAX_VALUE);
                index++;
                for (int i = 0; i < N; i++) {
                    long[] rowIndexA = { rowIndex };
                    Long l = Bits.createLong(i, N);
                    cells.add(new Tuple2<Cell, long[]>(new Cell(l, strValues[i]), rowIndexA));
                }
                return cells;
            }
        }).mapToPair(new PairFunction<Tuple2<Cell, long[]>, Cell, long[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Cell, long[]> call(Tuple2<Cell, long[]> t) throws Exception {
                return t;
            }
        });
    }

    /**
     * This method implements the apriori algorithm. It uses the
     * currentLevelPLIs to produce new tuples where the key is the input key
     * reduces by one character. The value of the new tuple is the complete old
     * tuple. Then all tuples are grouped by the key. In the end the method
     * combines all combinations and intersects the PLIs.
     * 
     * @param pliHashMap
     * @param candidates
     * @param nodes
     *            number of partions
     * @return true - unique, false -> non -unique
     */
    private static JavaPairRDD<Long, Boolean> generateNextLevelPLIs(final HashMap<Long, long[]> pliHashMap,
            final JavaRDD<Long> candidates, int nodes) {
        candidates.repartition(nodes);
        return candidates.mapToPair(new MapToIntersectedPLI(pliHashMap));
    }

    /**
     * This method creates position list indices for single columns.
     * 
     * @param cellValues
     *            Input Cell = column + value, Long = row
     * @return
     */
    private static JavaPairRDD<Long, long[]> createPLIs(JavaPairRDD<Cell, long[]> cellValues) {
        return cellValues.reduceByKey(new Function2<long[], long[], long[]>() {
            private static final long serialVersionUID = 1L;

            @Override
            public long[] call(long[] v1, long[] v2) throws Exception {
                return ArrayUtils.addAll(v1, v2);
            }
        }).mapToPair(new SkipCellValues()).reduceByKey(new ReduceDuplicates());
    }

}
