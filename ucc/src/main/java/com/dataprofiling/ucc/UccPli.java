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
 * @author pjung, jpollak
 *
 */
public class UccPli {

	final static String delimiter = ",";

	// private static Map<BitSet, Set<BitSet>> addedColumnCount = null;

	// TODO: implement trie for keeping track of min uniques and subset
	// uniqueness checking
	// private static PatriciaTrie<BitSet> uniques = new PatriciaTrie<BitSet>();

	public static void main(String[] args) throws Exception {
		int round = Integer.MAX_VALUE;
		if (args.length > 1 && args[1] != null)
			round = Integer.valueOf(args[1]);
		long start = System.currentTimeMillis();

		// encode column combinations as bit sets
		Set<BitSet> minUcc = new HashSet<>();

		final String inputFile = args[0];
		// N = getColumnCount(inputFile);
		JavaSparkContext spark = createSparkContext();
		System.out.println("Created context: "
				+ (System.currentTimeMillis() - start) + "ms");

		JavaRDD<String> file = spark.textFile(inputFile);

		System.out.println("Read file: " + (System.currentTimeMillis() - start)
				+ "ms");

		String firstLine = file.first();
		int n = firstLine.split(delimiter).length;
		for (int i = 0; i < n; i++) {
			BitSet bitSet = new BitSet(n);
			bitSet.set(i);
			minUcc.add(bitSet);
		}
		long before = System.currentTimeMillis() - start;
		System.out.println("Before createCellValues: " + before + "ms");
		long start2 = System.currentTimeMillis();
		JavaRDD<Cell> cellValues = createCellValues(file);
		System.out.println("After createCellValues: "
				+ (System.currentTimeMillis() - start2) + "ms");

		// get PLI for non unique columns
		JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumns = createPLIs(cellValues);
		// every round the single column PLIs will be combined with the current
		// level column combinations
		plisSingleColumns.cache();
		List<Tuple2<BitSet, List<LongArrayList>>> nonUniques = plisSingleColumns
				.collect();

		// System.out.println(nonUniques);

		for (Tuple2<BitSet, List<LongArrayList>> nonUnique : nonUniques) {
			minUcc.remove(nonUnique._1);
		}

		// JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumnsCopy =
		// plisSingleColumns;
		JavaPairRDD<BitSet, List<LongArrayList>> currentLevelPLIs = plisSingleColumns;
		boolean done = false;

		// List<Tuple2<BitSet, List<LongArrayList>>> singlePLIs =
		// plisSingleColumns
		// .collect();
		// System.out.println(singlePLIs);

		// addedColumnCount = new HashMap<BitSet, Set<BitSet>>();
		int current = 0;
		while (!done && current < round) {
			current++;
			long startLoop = System.currentTimeMillis();
			Broadcast<Set<BitSet>> broadcastMinUCC = spark.broadcast(minUcc);
			System.out.println("Broadcast took: "
					+ (System.currentTimeMillis() - startLoop) + "ms");

			// generate candidates
			long startIntersection = System.currentTimeMillis();
			JavaPairRDD<BitSet, List<LongArrayList>> intersectedPLIs = generateNextLevelPLIs(
					currentLevelPLIs, broadcastMinUCC.value()).cache();
			// intersectedPLIs.collect();
			System.out.println("Generation/Intersection took: "
					+ (System.currentTimeMillis() - startIntersection) + "ms");

			if (intersectedPLIs.isEmpty()) {
				// abort processing once there are no new candidates to check
				done = true;
				break;
			}

			long filterTime = System.currentTimeMillis();
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
					}).cache();
			nonUniqueCombinations.collect();
			System.out.println("filter nonuniques: "
					+ (System.currentTimeMillis() - filterTime) + "ms");

			long findnewminuccs = System.currentTimeMillis();
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
			System.out.println("Finding new minUccs (collect) took:"
					+ (System.currentTimeMillis() - findnewminuccs) + "ms");

			long addTime = System.currentTimeMillis();
			for (Tuple2<BitSet, List<LongArrayList>> tuple2 : newMinUCC) {
				minUcc.add(tuple2._1);
			}
			System.out.println("adding new min Uccs:"
					+ (System.currentTimeMillis() - addTime) + "ms");

			// prepare new round of candidate generation
			currentLevelPLIs = nonUniqueCombinations;
			System.out.println("Finished another iteration: "
					+ (System.currentTimeMillis() - startLoop) + "ms");
		}

		System.out.println("Runtime: " + (System.currentTimeMillis() - start)
				/ 1000 + "s");
		System.out.println("Minimal Unique Column Combinations: " + minUcc);
		spark.close();
	}

	private static JavaSparkContext createSparkContext() {
		SparkConf config = new SparkConf().setAppName("UCC");
		config.set("spark.hadoop.validateOutputSpecs", "false");
		return new JavaSparkContext(config);
	}

	/**
	 * This method reads the given file row by row and create a spark RDD of
	 * Cells.
	 * 
	 * @param file
	 *            spark file
	 * @return
	 */
	private static JavaRDD<Cell> createCellValues(JavaRDD<String> file) {
		// return file.zipWithIndex().flatMap(new
		// FlatMapFunction<Tuple2<String,Long>, Cell>() {
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Iterable<Cell> call(Tuple2<String, Long> t)
		// throws Exception {
		// String[] strValues = t._1.split(delimiter);
		// int N = strValues.length;
		// List<Cell> Cells = new ArrayList<Cell>();
		// for (int i = 0; i < N; i++) {
		// BitSet bs = new BitSet(N);
		// bs.set(i);
		// Cells.add(new Cell(bs, t._2, strValues[i]));
		// }
		// return Cells;
		// }
		// });

		return file.flatMap(new FlatMapFunction<String, Cell>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Cell> call(String s) {
				// under the assumption of horizontal partitioning
				// a local row index should work for combining multiple
				// columns
				int rowIndex = s.hashCode() + ((int) (Math.random() * 100));
				String[] strValues = s.split(delimiter);
				int N = strValues.length;
				List<Cell> Cells = new ArrayList<Cell>();
				for (int i = 0; i < N; i++) {
					BitSet bs = new BitSet(N);
					bs.set(i);
					Cells.add(new Cell(bs, rowIndex, strValues[i]));
				}
				rowIndex++;
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
		// JavaPairRDD<BitSet, List<Tuple2<BitSet, List<LongArrayList>>>> a =
		// currentLevelPLIs
		// .mapToPair(
		// new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet,
		// List<Tuple2<BitSet, List<LongArrayList>>>>() {
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<BitSet, List<Tuple2<BitSet, List<LongArrayList>>>>
		// call(
		// Tuple2<BitSet, List<LongArrayList>> t)
		// throws Exception {
		// List<Tuple2<BitSet, List<LongArrayList>>> list = new
		// ArrayList<Tuple2<BitSet,List<LongArrayList>>>();
		// list.add(t);
		// BitSet bitSet = (BitSet) t._1().clone();
		// int highestBit = bitSet.previousSetBit(bitSet
		// .length());
		// bitSet.clear(highestBit);
		// return new Tuple2<BitSet, List<Tuple2<BitSet, List<LongArrayList>>>>(
		// bitSet, list);
		// }
		// }).reduceByKey(new
		// Function2<List<Tuple2<BitSet,List<LongArrayList>>>,
		// List<Tuple2<BitSet,List<LongArrayList>>>,
		// List<Tuple2<BitSet,List<LongArrayList>>>>() {
		//
		// @Override
		// public List<Tuple2<BitSet, List<LongArrayList>>> call(
		// List<Tuple2<BitSet, List<LongArrayList>>> v1,
		// List<Tuple2<BitSet, List<LongArrayList>>> v2) throws Exception {
		// v1.addAll(v2);
		// return v1;
		// }
		// });
		long mapGrouptime = System.currentTimeMillis();
		JavaPairRDD<BitSet, Iterable<Tuple2<BitSet, List<LongArrayList>>>> a = currentLevelPLIs
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
								return new Tuple2<BitSet, Tuple2<BitSet, List<LongArrayList>>>(
										bitSet, t);
							}
						}).groupByKey();

		a.collect();
		System.out.println("maptopair + group time: "
				+ (System.currentTimeMillis() - mapGrouptime) + "ms");
		// System.out.println("size of groupedByKeyList " + a.collect().size());

		long flatMapIntersect = System.currentTimeMillis();
		JavaRDD<Tuple2<BitSet, List<LongArrayList>>> b = a.flatMap(
		// TODO: check how many times we get here for one round !!!

				new FlatMapFunction<Tuple2<BitSet, Iterable<Tuple2<BitSet, List<LongArrayList>>>>, Tuple2<BitSet, List<LongArrayList>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<BitSet, List<LongArrayList>>> call(
							Tuple2<BitSet, Iterable<Tuple2<BitSet, List<LongArrayList>>>> t)
							throws Exception {
						long startgeneration = System.currentTimeMillis();

						List<Tuple2<BitSet, List<LongArrayList>>> newCandidates = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
						List<Tuple2<BitSet, List<LongArrayList>>> tList = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
						Iterator<Tuple2<BitSet, List<LongArrayList>>> it = t._2
								.iterator();

						while (it.hasNext()) {
							tList.add(it.next());
						}

						for (int i = 0; i < tList.size() - 1; i++) {
							for (int j = i + 1; j < tList.size(); j++) {
								Tuple2<BitSet, List<LongArrayList>> intersection = combine(
										tList.get(i), tList.get(j));
								if (intersection._2 != null) {
									newCandidates.add(intersection);
								}
							}
						}

						// TODO: output veeery many times: how often does this
						// really need to be run per iteration?
						System.out
								.println("Generation/Intersection inside took: "
										+ (System.currentTimeMillis() - startgeneration)
										+ "ms");
						return newCandidates;
					}

					// new FlatMapFunction<Tuple2<BitSet, List<Tuple2<BitSet,
					// List<LongArrayList>>>>, Tuple2<BitSet,
					// List<LongArrayList>>>() {
					// private static final long serialVersionUID = 1L;
					//
					// @Override
					// public Iterable<Tuple2<BitSet, List<LongArrayList>>>
					// call(
					// Tuple2<BitSet, List<Tuple2<BitSet, List<LongArrayList>>>>
					// t)
					// throws Exception {
					// long startgeneration = System.currentTimeMillis();
					//
					// List<Tuple2<BitSet, List<LongArrayList>>> newCandidates =
					// new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
					//
					// for (int i = 0; i < t._2.size() - 1; i++) {
					// for (int j = i + 1; j < t._2.size(); j++) {
					// Tuple2<BitSet, List<LongArrayList>> intersection =
					// combine(t._2.get(i),
					// t._2.get(j));
					// if (intersection._2 != null) {
					// newCandidates.add(intersection);
					// }
					// }
					// }
					//
					// // TODO: output veeery many times: how often does this
					// really need to be run per iteration?
					// System.out.println("Generation/Intersection inside took: "
					// + (System.currentTimeMillis() - startgeneration) + "ms");
					// return newCandidates;
					// }

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
				});
		b.collect();

		System.out.println("flatmap which includes intersection time: "
				+ (System.currentTimeMillis() - flatMapIntersect) + "ms");

		return b.mapToPair(new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet, List<LongArrayList>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<BitSet, List<LongArrayList>> call(
					Tuple2<BitSet, List<LongArrayList>> t) throws Exception {
				return new Tuple2<BitSet, List<LongArrayList>>(t._1, t._2);
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
		long uniqueValueCount = 0;
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

	private static void updateMap(Map<LongPair, LongArrayList> map,
			long rowIndex, LongPair pair) {
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
		Long2LongOpenHashMap hashedPLI = new Long2LongOpenHashMap(
				clusters.size());
		long uniqueValueCount = 0;
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

		JavaPairRDD<Cell, LongArrayList> cell2Positions = cellValues.mapToPair(
				new PairFunction<UccPli.Cell, Cell, LongArrayList>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Cell, LongArrayList> call(Cell t)
							throws Exception {
						LongArrayList rowIndex = new LongArrayList();
						rowIndex.add(t.rowIndex);
						return new Tuple2<UccPli.Cell, LongArrayList>(t,
								rowIndex);
					}
				}).reduceByKey(
				new Function2<LongArrayList, LongArrayList, LongArrayList>() {
					private static final long serialVersionUID = 1L;

					@Override
					public LongArrayList call(LongArrayList v1, LongArrayList v2)
							throws Exception {
						v1.addAll(v2);
						return v1;
					}
				});

		JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumns = cell2Positions
				.filter(new Function<Tuple2<Cell, LongArrayList>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<UccPli.Cell, LongArrayList> v1)
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

		return plisSingleColumns;
	}

	static class Cell implements Serializable {
		private static final long serialVersionUID = 1L;
		BitSet columnIndex;
		long rowIndex;
		// TODO: data type
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
