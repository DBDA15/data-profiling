package com.dataprofiling.ucc;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * Distributed UCC Discovery.
 * 
 * Possible optimizations:
 *  trie for keeping track of min uniques and subset
 *  
 * @author pjung, jpollak
 *
 */
public class UccDiscovery {
	
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		
		// by default, all lattice levels will be checked
		int levelsToCheck = Integer.MAX_VALUE;
		
		if (args.length < 2 || args[0] == null || args[1] == null) {
			System.err.println("Missing parameters!");
			System.exit(-1);
		}
		final String inputFile = args[0];
		String delimiter = args[1];
		
		if (args.length > 2 && args[2] != null) {
			levelsToCheck = Integer.valueOf(args[2]);
		}
		
		// encode column combinations as bit sets
		Set<BitSet> minUcc = new HashSet<>();

		JavaSparkContext spark = createSparkContext();
		JavaRDD<String> file = spark.textFile(inputFile);

		// let slaves know what the column delimiter is
		Broadcast<String> bcDelimiter = spark.broadcast(delimiter);
		String localDelimiter = bcDelimiter.value();
		
		String firstLine = file.first();
		int n = firstLine.split(delimiter).length;
		
		for (int i = 0; i < n; i++) {
			BitSet bitSet = new BitSet(n);
			bitSet.set(i);
			minUcc.add(bitSet);
		}
		System.out.println("MinuCC works? : " + minUcc);
		
		JavaRDD<Cell> cellValues = createCellValues(file, localDelimiter);

		// get PLI for non unique columns
		JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumns = createPLIs(cellValues); // TODO: caching?
		System.out.println(plisSingleColumns.collect());
		
		List<Tuple2<BitSet, List<LongArrayList>>> nonUniques = plisSingleColumns.collect();
		for (Tuple2<BitSet, List<LongArrayList>> nonUnique : nonUniques) {
			minUcc.remove(nonUnique._1);
		}

		JavaPairRDD<BitSet, List<LongArrayList>> currentLevelPLIs = plisSingleColumns;
		Broadcast<Set<BitSet>> broadcastMinUCC = spark.broadcast(minUcc);
		Set<BitSet> localMinUcc = broadcastMinUCC.value(); // TODO: check if slaves receive it
		boolean done = false;
		int currentLevel = 0;
		
		while (!done && currentLevel < levelsToCheck) {
			long startLoop = System.currentTimeMillis();
			currentLevel++;

			// generate candidates
			JavaPairRDD<BitSet, List<LongArrayList>> intersectedPLIs = generateNextLevelPLIs(
					currentLevelPLIs, localMinUcc);
			// intersectedPLIs.cache(); TODO: caching?
			
			if (intersectedPLIs.isEmpty()) {
				// abort processing once there are no new candidates to check
				done = true;
				break;
			}

			// filter for non uniques and save uniques
			JavaPairRDD<BitSet, List<LongArrayList>> nonUniqueCombinations = intersectedPLIs
					.filter(new Function<Tuple2<BitSet, List<LongArrayList>>, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(
								Tuple2<BitSet, List<LongArrayList>> v1)
								throws Exception {
							if (!v1._2.isEmpty()) {
								// candidate is not unique if there are any
								// redundant values
								return true;
							}
							return false;
						}
					}); // cache() ?

			List<Tuple2<BitSet, List<LongArrayList>>> newMinUCC = intersectedPLIs
					.filter(new Function<Tuple2<BitSet, List<LongArrayList>>, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(
								Tuple2<BitSet, List<LongArrayList>> v1)
								throws Exception {
							if (!v1._2.isEmpty()) {
								// candidate is not unique if there are any
								// redundant values
								return false;
							}
							return true;
						}
					}).collect();
			
			for (Tuple2<BitSet, List<LongArrayList>> tuple2 : newMinUCC) {
				minUcc.add(tuple2._1);
			}
			
			// prepare new round of candidate generation
			currentLevelPLIs = nonUniqueCombinations;
			System.out.println("Finished another iteration: "
					+ (System.currentTimeMillis() - startLoop) + "ms");
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Runtime: " + (end - start) / 1000 + "s");
		System.out.println("Minimal Unique Column Combinations: " + minUcc);
		spark.close();
	}

	private static JavaSparkContext createSparkContext() {
		SparkConf config = new SparkConf().setAppName("de.hpi.dbda.UccDiscovery");
		config.set("spark.hadoop.validateOutputSpecs", "false");
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
	private static JavaRDD<Cell> createCellValues(JavaRDD<String> file,
			final String delimiter) {
		return file.zipWithIndex().flatMap(
				new FlatMapFunction<Tuple2<String, Long>, Cell>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Cell> call(Tuple2<String, Long> t)
							throws Exception {
						String[] strValues = t._1.split(delimiter);
						int N = strValues.length;
						List<Cell> Cells = new ArrayList<Cell>();
						
						for (int i = 0; i < N; i++) {
							BitSet bs = new BitSet(N);
							bs.set(i);
							Cells.add(new Cell(bs, t._2, strValues[i]));
						}
						return Cells;
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
	 * @param currentLevelPLIs
	 * @param minUCC
	 * @return nextLevelPLIs (which may include some subsets which are already
	 *         unique)
	 */
	private static JavaPairRDD<BitSet, List<LongArrayList>> generateNextLevelPLIs(
			JavaPairRDD<BitSet, List<LongArrayList>> currentLevelPLIs,
			final Set<BitSet> minUCC) {
		
		// apriori based candidate generation
		return currentLevelPLIs
				.mapToPair(
						new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet, Tuple2<BitSet, List<LongArrayList>>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<BitSet, Tuple2<BitSet, List<LongArrayList>>> call(
									Tuple2<BitSet, List<LongArrayList>> t)
									throws Exception {

								BitSet bitSet = (BitSet) t._1().clone();
								int highestBit = bitSet.previousSetBit(bitSet
										.length());
								bitSet.clear(highestBit);
								
								// column combination prefix -> old tuple
								return new Tuple2<BitSet, Tuple2<BitSet, List<LongArrayList>>>(
										bitSet, t);
							}
						})
						// combine those combinations that share the same prefix
						.groupByKey(8) // TODO: check if good
						// create tuples for new column combinations with intersected plis
						.flatMap(

				new FlatMapFunction<Tuple2<BitSet, Iterable<Tuple2<BitSet, List<LongArrayList>>>>, Tuple2<BitSet, List<LongArrayList>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<BitSet, List<LongArrayList>>> call(
							Tuple2<BitSet, Iterable<Tuple2<BitSet, List<LongArrayList>>>> t)
							throws Exception {

						List<Tuple2<BitSet, List<LongArrayList>>> newCandidates = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
						
						List<Tuple2<BitSet, List<LongArrayList>>> tupleList = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
						Iterator<Tuple2<BitSet, List<LongArrayList>>> it = t._2.iterator();

						while (it.hasNext()) {
							tupleList.add(it.next());
						}

						for (int i = 0; i < tupleList.size() - 1; i++) {
							for (int j = i + 1; j < tupleList.size(); j++) {
								Tuple2<BitSet, List<LongArrayList>> intersection = combine(
										tupleList.get(i), tupleList.get(j));
								if (intersection._2 != null) {
									newCandidates.add(intersection);
								}
							}
						}		
						return newCandidates;
					}

					/**
					 * This method combines two Indices, PLIs to one (index,
					 * pli)
					 */
					private Tuple2<BitSet, List<LongArrayList>> combine(
							Tuple2<BitSet, List<LongArrayList>> outer,
							Tuple2<BitSet, List<LongArrayList>> inner) {
						BitSet newColumCombination = (BitSet) outer._1.clone();
						newColumCombination.or(inner._1);
						List<LongArrayList> newPLI = null;

						// do subset check
						if (!isSubsetUnique(newColumCombination, minUCC)) {
							newPLI = intersect(outer._2, inner._2);
						}

						return new Tuple2<BitSet, List<LongArrayList>>(
								newColumCombination, newPLI);
					}
						})
				.mapToPair(
						new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet, List<LongArrayList>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<BitSet, List<LongArrayList>> call(
									Tuple2<BitSet, List<LongArrayList>> t)
									throws Exception {
								return new Tuple2<BitSet, List<LongArrayList>>(
										t._1, t._2);
							}
						});
	}

	private static List<LongArrayList> intersect(List<LongArrayList> thisPLI,
			List<LongArrayList> otherPLI) {
		// thisPLI: e.g. {{1,2,3},{4,5}}
		// otherPLI: e.g. {{1,3},{2,5}}

		// intersected PLI for above example: {{1,3}
		List<LongArrayList> intersection = new ArrayList<>();

		Long2LongOpenHashMap hashedPLI = asHashMap(thisPLI);
		Map<LongPair, LongArrayList> map = new HashMap<>();
		buildMap(otherPLI, hashedPLI, map);

		for (LongArrayList cluster : map.values()) {
			if (cluster.size() < 2) {
				continue;
			}
			intersection.add(cluster);
		}
		return intersection;
	}

	private static void buildMap(List<LongArrayList> otherPLI,
			Long2LongOpenHashMap hashedPLI, Map<LongPair, LongArrayList> map) {
		int uniqueValueCount = 0;
		for (LongArrayList sameValues : otherPLI) {
			for (long rowIndex : sameValues) {
				if (hashedPLI.containsKey(rowIndex)) {
					LongPair pair = new LongPair(uniqueValueCount,
							hashedPLI.get(rowIndex));
					updateMap(map, rowIndex, pair);
				}
			}
			uniqueValueCount++;
		}
	}

	private static void updateMap(Map<LongPair, LongArrayList> map, long rowIndex,
			LongPair pair) {
		if (map.containsKey(pair)) {
			LongArrayList currentList = map.get(pair);
			currentList.add(rowIndex);
		} else {
			LongArrayList newList = new LongArrayList();
			newList.add(rowIndex);
			map.put(pair, newList);
		}
	}

	/**
	 * Returns the position list index in a map representation. Every row index
	 * maps to a value reconstruction. As the original values are unknown they
	 * are represented by a counter. The position list index ((0, 1), (2, 4),
	 * (3, 5)) would be represented by {0=0, 1=0, 2=1, 3=2, 4=1, 5=2}.
	 *
	 * @return the pli as hash map
	 */
	private static Long2LongOpenHashMap asHashMap(List<LongArrayList> clusters) {
		Long2LongOpenHashMap hashedPLI = new Long2LongOpenHashMap(clusters.size());
		int uniqueValueCount = 0;
		for (LongArrayList sameValues : clusters) {
			for (long rowIndex : sameValues) {
				hashedPLI.put(rowIndex, uniqueValueCount);
			}
			uniqueValueCount++;
		}
		return hashedPLI;
	}

	/**
	 * Check if any of the minimal uniques is completely contained in the column
	 * combination. If so, a subset is already unique.
	 * 
	 * TODO: inefficient due to comparisons to all minimal uniques
	 * 
	 * @param columnCombination
	 * @param minUCC
	 * @return true if columnCombination contains unique subset, false otherwise
	 */
	private static boolean isSubsetUnique(BitSet columnCombination,
			Set<BitSet> minUCC) {
		for (BitSet bSet : minUCC) {
			// System.out.println("minUcc (in isSubsetUnique)" + minUCC);
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

	private static JavaPairRDD<BitSet, List<LongArrayList>> createPLIs(
			JavaRDD<Cell> cellValues) {
//		LongArrayList dummy = new LongArrayList();
		return cellValues
				.mapToPair(
						new PairFunction<UccDiscovery.Cell, Cell, LongArrayList>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Cell, LongArrayList> call(Cell t)
									throws Exception {
								LongArrayList rowIndex = new LongArrayList();
								rowIndex.add(t.rowIndex);
								return new Tuple2<UccDiscovery.Cell, LongArrayList>(
										t, rowIndex);
							}
						})
//						.foldByKey(dummy, new Function2<LongArrayList, LongArrayList, LongArrayList>() {
//							
//							@Override
//							public LongArrayList call(LongArrayList v1, LongArrayList v2)
//									throws Exception {
//								v1.addAll(v2);
//								return v1;
//							}
//						})
				.reduceByKey(
						new Function2<LongArrayList, LongArrayList, LongArrayList>() {
							private static final long serialVersionUID = 1L;

							@Override
							public LongArrayList call(LongArrayList v1,
									LongArrayList v2) throws Exception {
								v1.addAll(v2);
								return v1;
							}
						})
				.filter(new Function<Tuple2<Cell, LongArrayList>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(
							Tuple2<UccDiscovery.Cell, LongArrayList> v1)
							throws Exception {
						return v1._2.size() != 1;
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<Cell, LongArrayList>, BitSet, List<LongArrayList>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<BitSet, List<LongArrayList>> call(
									Tuple2<Cell, LongArrayList> v1)
									throws Exception {
								List<LongArrayList> listOfRedundantValueLists = new ArrayList<LongArrayList>();
								listOfRedundantValueLists.add(v1._2);
								
								return new Tuple2<BitSet, List<LongArrayList>>(
										v1._1.columnIndex,
										listOfRedundantValueLists);
							}
						})
				.reduceByKey(
						new Function2<List<LongArrayList>, List<LongArrayList>, List<LongArrayList>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public List<LongArrayList> call(
									List<LongArrayList> v1,
									List<LongArrayList> v2) throws Exception {
								v1.addAll(v2);
								return v1;
							}
						});
	}

	static class Cell implements Serializable {
		private static final long serialVersionUID = 1L;
		BitSet columnIndex;
		// row index not considered during equals comparison
		long rowIndex;
		String value;

		public Cell(BitSet columnIndex, long rowIndex, String value) {
			this.columnIndex = columnIndex;
			this.rowIndex = rowIndex;
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Cell attr = (Cell) obj;
			if (attr.columnIndex.equals(this.columnIndex)
					&& attr.value.equals(this.value)) {
				return true;
			}
			return false;

		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder(17, 31).append(columnIndex)
					.append(value).toHashCode();
		}

		public String toString() {
			return "[ " + this.columnIndex + ", " + this.rowIndex + ", "
					+ this.value + " ]";
		}

	}

}
