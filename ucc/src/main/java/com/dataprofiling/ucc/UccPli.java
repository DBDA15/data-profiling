package com.dataprofiling.ucc;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
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

import scala.Tuple2;

/**
 * Distributed UCC Discovery.
 * 
 * @author pjung, jpollak
 *
 */
public class UccPli {

	// global ucc state variables
	private static int N = 0;
	// encode column combinations as bit sets
	private static Set<BitSet> minUcc = new HashSet<>();
	private static final Set<BitSet> nonUniqueColumns = new HashSet<BitSet>();
	private static Map<BitSet, Set<BitSet>> addedColumnCount = null;

	// TODO: implement trie for keeping track of min uniques and subset
	// uniqueness checking
	// private static PatriciaTrie<BitSet> uniques = new PatriciaTrie<BitSet>();

	private static JavaSparkContext createSparkContext() {
		SparkConf config = new SparkConf().setAppName("UCC");
		config.set("spark.hadoop.validateOutputSpecs", "false");
		return new JavaSparkContext(config);
	}

	/**
	 * This method reads first row to determine number of columns.
	 * 
	 * @param inputFile
	 *            name of input file (including path)
	 * @return number of columns
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static int getColumnCount(final String inputFile)
			throws FileNotFoundException, IOException {
		FileReader reader = new FileReader(inputFile);
		BufferedReader buffer = new BufferedReader(reader);
		String firstRow = buffer.readLine();
		buffer.close();
		return firstRow.split(",").length;
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
		return file.flatMap(new FlatMapFunction<String, Cell>() {
			private static final long serialVersionUID = 1L;
			long rowIndex = 0;

			public Iterable<Cell> call(String s) {
				// under the assumption of horizontal partitioning
				// a local row index should work for combining multiple
				// columns

				String[] strValues = s.split(",");
				List<Cell> Cells = new ArrayList<Cell>();
				for (int i = 0; i < N; i++) {
					BitSet bs = new BitSet(N);
					bs.set(i);
					Cells.add(new Cell(bs, rowIndex, strValues[i]));

					if (rowIndex == 0) {
						// only during first row, make all columns potential
						// uniques
						minUcc.add(bs);
						// after checking all first level candidates, those who
						// are not unique will be removed
					}
				}
				rowIndex++;
				return Cells;
			}
		});
	}

	public static void main(String[] args) throws Exception {
		final String inputFile = args[0] + "/" + args[1] + ".csv";
		N = getColumnCount(inputFile);
		JavaSparkContext spark = createSparkContext();
		JavaRDD<String> file = spark.textFile(inputFile);
		JavaRDD<Cell> cellValues = createCellValues(file);

		// get PLI for non unique columns
		JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumns = createPLIs(cellValues);
		// every round the single column PLIs will be combined with the current level column combinations
		plisSingleColumns.cache();
		List<Tuple2<BitSet, List<LongArrayList>>> nonUniques = plisSingleColumns
				.collect();
		//System.out.println(nonUniques);

		for (Tuple2<BitSet, List<LongArrayList>> nonUnique : nonUniques) {
			nonUniqueColumns.add(nonUnique._1);
			minUcc.remove(nonUnique._1);
		}

		// JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumnsCopy =
		// plisSingleColumns;
		JavaPairRDD<BitSet, List<LongArrayList>> currentLevelPLIs = plisSingleColumns;
		boolean done = false;

		while (!done) {
			// reset before generating a new round
			addedColumnCount = new HashMap<BitSet, Set<BitSet>>();

			// ----------------------------------------------------------------------------------------------------------------------------
			// 1. CANDIDATE GENERATION
			
			JavaPairRDD<BitSet, List<LongArrayList>> newCandidates2PreviousColumnCombinationPLIs = currentLevelPLIs
					.flatMap(
		new FlatMapFunction<Tuple2<BitSet, List<LongArrayList>>, Tuple2<BitSet, List<LongArrayList>>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Iterable<Tuple2<BitSet, List<LongArrayList>>> call(
										Tuple2<BitSet, List<LongArrayList>> t)
										throws Exception {
									List<Tuple2<BitSet, List<LongArrayList>>> newCandidates = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();

									BitSet columnCombination = t._1;
									List<LongArrayList> thisLevelPLI = t._2();

									Set<BitSet> nextLevelCandidates = generateNextLevelFor(columnCombination);

									for (BitSet candidate : nextLevelCandidates) {
										newCandidates
												.add(new Tuple2<BitSet, List<LongArrayList>>(
														candidate, thisLevelPLI));

										// count how many times which unique
										// column has been added to form a new
										// candidate
										BitSet addedColumn = (BitSet) candidate
												.clone();
										addedColumn.xor(columnCombination);
										if (!addedColumnCount
												.containsKey(addedColumn)) {
											addedColumnCount.put(addedColumn,
													new HashSet<BitSet>());
										}
										addedColumnCount.get(addedColumn).add(
												candidate);

									}
									return newCandidates;
								}

							})
					.mapToPair(
							new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet, List<LongArrayList>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<BitSet, List<LongArrayList>> call(
										Tuple2<BitSet, List<LongArrayList>> t)
										throws Exception {
									return new Tuple2<BitSet, List<LongArrayList>>(
											t._1, t._2);
								}
							});

			if (newCandidates2PreviousColumnCombinationPLIs.isEmpty()) {
				// abort processing once there are no new candidates to check
				done = true;
				break;
			}

			JavaPairRDD<BitSet, List<LongArrayList>> newCandidates2UniqueColumnPLIs = plisSingleColumns
					.flatMap(
							new FlatMapFunction<Tuple2<BitSet, List<LongArrayList>>, Tuple2<BitSet, List<LongArrayList>>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Iterable<Tuple2<BitSet, List<LongArrayList>>> call(
										Tuple2<BitSet, List<LongArrayList>> t)
										throws Exception {

									List<Tuple2<BitSet, List<LongArrayList>>> newCandidates = new ArrayList<Tuple2<BitSet, List<LongArrayList>>>();
									BitSet nonUniqueColumn = t._1;
									List<LongArrayList> nonUniqueColumnPLI = t._2;

									if (addedColumnCount
											.containsKey(nonUniqueColumn)) {
										Set<BitSet> candidates = addedColumnCount
												.get(nonUniqueColumn);
										for (BitSet candidate : candidates) {
											newCandidates
													.add(new Tuple2<BitSet, List<LongArrayList>>(
															candidate,
															nonUniqueColumnPLI));
										}
									}

									return newCandidates;
								}
							})
					.mapToPair(
							new PairFunction<Tuple2<BitSet, List<LongArrayList>>, BitSet, List<LongArrayList>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<BitSet, List<LongArrayList>> call(
										Tuple2<BitSet, List<LongArrayList>> t)
										throws Exception {
									return new Tuple2<BitSet, List<LongArrayList>>(
											t._1, t._2);
								}

							});

//			System.out.println("1: "
//					+ newCandidates2PreviousColumnCombinationPLIs.collect());
//			System.out
//					.println("2: " + newCandidates2UniqueColumnPLIs.collect());

			// ----------------------------------------------------------------------------------------------------------------------------
			// 2. INTERSECTION OF CANDIDATE PLIS
			
			// combine position lists from new candidates
			JavaPairRDD<BitSet, List<LongArrayList>> intersectedPLIs = newCandidates2PreviousColumnCombinationPLIs
					.join(newCandidates2UniqueColumnPLIs)
					.mapToPair(
							new PairFunction<Tuple2<BitSet, Tuple2<List<LongArrayList>, List<LongArrayList>>>, BitSet, List<LongArrayList>>() {

								/**
								 * 
								 */
								private static final long serialVersionUID = 1L;

								@Override
								public Tuple2<BitSet, List<LongArrayList>> call(
										Tuple2<BitSet, Tuple2<List<LongArrayList>, List<LongArrayList>>> t)
										throws Exception {

									return new Tuple2<BitSet, List<LongArrayList>>(
											t._1, intersect(t._2._1, t._2._2));
								}
							});
			
			// ----------------------------------------------------------------------------------------------------------------------------
			// 3. FILTER UNIQUES AND NON UNIQUES
			
			// filter for non uniques and save uniques
			JavaPairRDD<BitSet, List<LongArrayList>> nonUniqueCombinations = intersectedPLIs
					.filter(new Function<Tuple2<BitSet, List<LongArrayList>>, Boolean>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(
								Tuple2<BitSet, List<LongArrayList>> v1)
								throws Exception {
							if (!v1._2.isEmpty()) {
								// candidate is not unique if there are any
								// redundant values
								return true;
							} else {
								minUcc.add(v1._1());
								return false;
							}
						}
					}).cache();
			
			// prepare new round of candidate generation
			currentLevelPLIs = nonUniqueCombinations;
		}

		System.out.println("Minimal Unique Column Combinations: " + minUcc);
		spark.close();
	}

	/**
	 * New candidates are build from left to right to avoid having different
	 * candidates from the same level generate the same next level candidate, as
	 * AB and BC could otherwise both lead to ABC. Example 3 Columns A,B,C 1.
	 * level: A, B, C 2. level: A-> AB, AC - B-> BC 3. level: AB-> ABC
	 * 
	 * @param columnCombination
	 * @return
	 */
	private static Set<BitSet> generateNextLevelFor(BitSet columnCombination) {
		Set<BitSet> nextLevelCandidates = new HashSet<BitSet>();

		int cardinalityBefore = columnCombination.cardinality();
		int highestBitBefore = columnCombination
				.previousSetBit(columnCombination.size());

		for (BitSet column : nonUniqueColumns) {
			BitSet columnCombinationCopy = (BitSet) columnCombination.clone();
			columnCombinationCopy.xor(column);

			if (columnCombinationCopy.cardinality() < cardinalityBefore) {
				// column is already contained
				continue;
			}

			// highest set bit needs to have changed to confirm a valid new set
			// A + B = AB valid 001 + 010 = 011 -> new highest bit
			// B + A = BA not valid 010 + 001 = 011
			// AB + C = ABC valid 011 + 100 = 111
			// AC + B = ACB not valid 101 + 010 = 111
			int newHighestBit = columnCombinationCopy
					.previousSetBit(columnCombinationCopy.size());

			if (newHighestBit <= highestBitBefore) {
				continue;
			}
			
			// verify that subset is not already unique
			if (!isSubsetUnique(columnCombinationCopy)) {
				nextLevelCandidates.add(columnCombinationCopy);
			}
		}

		return nextLevelCandidates;
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
	 * @return
	 */
	private static boolean isSubsetUnique(BitSet columnCombination) {
		for (BitSet bSet : minUcc) {
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

		JavaPairRDD<Cell, LongArrayList> cell2Positions = cellValues
				.mapToPair(
						new PairFunction<UccPli.Cell, Cell, LongArrayList>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Cell, LongArrayList> call(Cell t)
									throws Exception {
								LongArrayList rowIndex = new LongArrayList();
								rowIndex.add(t.rowIndex);
								return new Tuple2<UccPli.Cell, LongArrayList>(
										t, rowIndex);
							}
						})
				.reduceByKey(
						new Function2<LongArrayList, LongArrayList, LongArrayList>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public LongArrayList call(LongArrayList v1,
									LongArrayList v2) throws Exception {
								v1.addAll(v2);
								return v1;
							}
						});

		JavaPairRDD<BitSet, List<LongArrayList>> plisSingleColumns = cell2Positions
				.filter(new Function<Tuple2<Cell, LongArrayList>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<UccPli.Cell, LongArrayList> v1)
							throws Exception {
						return v1._2.size() != 1;
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<Cell, LongArrayList>, BitSet, List<LongArrayList>>() {

							/**
							 * 
							 */
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

							/**
							 * 
							 */
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

		// TODO rowIndex not considered yet
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
