package com.dataprofiling.ucc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
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
 */

/**
 * Optimization Ideas (not implemented) - use PLI - cache intermediate data -
 * pick random sample (e.g. 5 values) from each column, identify unique
 * candidates early - combine columns with fewest redundant values first to get
 * to UCC faster (-> prune more and reduce computation) -
 * 
 * @author pjung, jpollak
 *
 */
public class Ucc {
	private final static boolean DEBUG = true;

	// global ucc state variables
	private static int N = 0;
	// encode column combinations as bit sets
	private static List<BitSet> candidates = new ArrayList<>();
	private static Set<BitSet> minUcc = new HashSet<>();
	private static final List<BitSet> nonUniqueColumns = new ArrayList<BitSet>();
	// single columns are tested before
	private static int levelIndex = 2;

	private static JavaSparkContext createSparkContext() {
		SparkConf config = new SparkConf().setAppName("UCC");
		config.set("spark.hadoop.validateOutputSpecs", "false");
		return new JavaSparkContext(config);
	}

	public static void main(String[] args) throws Exception {
		final String inputFile = args[0] + "/" + args[1] + ".csv";
		JavaSparkContext spark = createSparkContext();
		JavaRDD<String> file = spark.textFile(inputFile);

		String firstLine = file.first();
		N = firstLine.split(",").length;
		// getColumnCount(inputFile);
		JavaRDD<Attribute> attributeValues = createAttributeValues(file);

		if (DEBUG)
			System.out.println(attributeValues.collect());

		checkSingleColumns(attributeValues);

		// from bottom to top, check each column combination on given lattice
		// level
		while (!candidates.isEmpty()) {
			if (DEBUG)
				System.out.println("levelIndex: " + levelIndex);

			final Broadcast<BitSet> candidate = spark.broadcast(candidates
					.remove(0));
			final BitSet candidateCopy = candidate.value();

			if (DEBUG)
				System.out.println("candidate to check: " + candidateCopy);

			doMultiColumnCheck(attributeValues, candidateCopy);
		}

		printMinimalUniques();
		spark.close();
	}

	private static void printMinimalUniques() {
		// print all minimal uniques
		System.out.println("Minimal Unique Column Combinations:");
		for (BitSet cc : minUcc) {
			System.out.println(cc);
		}
	}

	private static void doMultiColumnCheck(JavaRDD<Attribute> attributeValues,
			final BitSet candidateCopy) {
		if (DEBUG)
			System.out.println("do multi column check");

		// filter only values for the column to be checked
		JavaRDD<Attribute> filtered = attributeValues
				.filter(new Function<Ucc.Attribute, Boolean>() {
					private static final long serialVersionUID = 1L;

					// TODO: how expensive is this filter?
					@Override
					public Boolean call(Attribute v1) throws Exception {
						if (candidateCopy.intersects(v1.columnIndex)) {
							return true;
						}
						return false;
					}
				});

		if (DEBUG)
			System.out.println("filtered" + filtered.collect());

		JavaPairRDD<Integer, String> row2Value = filtered
				.mapToPair(new PairFunction<Ucc.Attribute, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Attribute t)
							throws Exception {
						// row -> value
						return new Tuple2<Integer, String>(t.rowIndex, t.value);
					}
				});

		// row -> combined value of multiple columns
		JavaPairRDD<Integer, String> columnCombination = row2Value
				.reduceByKey(new Function2<String, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(String v1, String v2) throws Exception {
						return v1 + "_#_" + v2;
					}
				});

		JavaPairRDD<String, Integer> nonUnique = columnCombination
				.mapToPair(
						new PairFunction<Tuple2<Integer, String>, String, Integer>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, Integer> call(
									Tuple2<Integer, String> t) throws Exception {
								return new Tuple2<String, Integer>(t._2, 1);
							}
						})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return Math.max(v1, v2);
					}
				}).filter(new Function<Tuple2<String, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Integer> v1)
							throws Exception {
						if (v1._2() != 1) {
							return true;
						}
						return false;
					}
				});

		List<Tuple2<String, Integer>> list = nonUnique.collect();
		if (DEBUG)
			System.out.println(list);

		if (nonUnique.count() == 0) {
			if (DEBUG)
				System.out.println("candidate UNIQUE! : " + candidateCopy);
			minUcc.add(candidateCopy);
			// remove over sets from candidates
			Iterator<BitSet> it = candidates.iterator();
			List<BitSet> toRemove = new ArrayList<BitSet>();
			while (it.hasNext()) {
				BitSet candidate = it.next();
				BitSet candidateCopyCopy = (BitSet) candidateCopy.clone();
				candidateCopyCopy.and(candidate);
				if (candidateCopy.equals(candidateCopyCopy)) {
					toRemove.add(candidate);
				}
			}
			candidates.removeAll(toRemove);
		} else {
			if (DEBUG)
				System.out.println("candidate not unique! : " + candidateCopy);
		}

		// generate next level, if column combination not size n yet
		if (candidateCopy.cardinality() < N) {
			// TODO only generate candidates if new level
			generateCandidates(candidateCopy);
		}
	}

	private static void generateCandidates(final BitSet candidateCopy) {
		Iterator<BitSet> it = nonUniqueColumns.iterator();
		while (it.hasNext()) {
			BitSet nonUniqueColumn = it.next();
			BitSet nonUniqueColumnCopy = (BitSet) nonUniqueColumn.clone();
			BitSet candidateCopyClone = (BitSet) candidateCopy.clone();

			nonUniqueColumnCopy.and(candidateCopyClone);

			if (nonUniqueColumnCopy.cardinality() == 1) {
				// dont combine because column is already
				// contained
			} else {
				candidateCopyClone.or(nonUniqueColumn);

				// add only if not in candidate list already
				if (!candidates.contains(candidateCopyClone)) {
					// and if subset is not already unique
					if (!uccIsSubSetOf(candidateCopyClone))
						candidates.add(candidateCopyClone);
				}
			}
		}
	}

	/**
	 * This method iterates over know uccs and returns whether one ucc is a
	 * subset of the given candidate. If a & b == a, a is a subset of b
	 * 
	 * @param candidate
	 * @return
	 */
	private static boolean uccIsSubSetOf(BitSet candidate) {
		Iterator<BitSet> it = minUcc.iterator();
		while (it.hasNext()) {
			BitSet ucc = it.next();
			BitSet uccCopy = (BitSet) ucc.clone();
			uccCopy.and(candidate);
			if (uccCopy.equals(ucc)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * do single-column checks, don't filter, just check all single columns
	 * simultaneously instead
	 * 
	 * @param attributeValues
	 */
	private static void checkSingleColumns(JavaRDD<Attribute> attributeValues) {
		// map attributes values
		JavaPairRDD<Attribute, Integer> pairs = attributeValues
				.mapToPair(new PairFunction<Attribute, Attribute, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Attribute, Integer> call(Attribute attr) {
						return new Tuple2<Attribute, Integer>(attr, 1);
					}
				});

		if (DEBUG) {
			Map<Attribute, Object> m = pairs.countByKey();
			for (Attribute a : m.keySet()) {
				System.out.print(a + ": ");
				System.out.println(m.get(a));
			}
			System.out.println(pairs.collect());
		}

		JavaPairRDD<Attribute, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		if (DEBUG)
			System.out.println("reduced: " + counts.collect());

		// key = column index, value is sum of occurrences of the same
		// attribute
		// value
		JavaPairRDD<BitSet, Integer> columnAttrCount = counts
				.mapToPair(new PairFunction<Tuple2<Attribute, Integer>, BitSet, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<BitSet, Integer> call(
							Tuple2<Attribute, Integer> t) throws Exception {
						return new Tuple2<BitSet, Integer>(t._1.columnIndex,
								t._2);
					}
				});

		if (DEBUG)
			System.out.println(columnAttrCount.collect());

		// key = column index, value = max of all attribute values sums
		JavaPairRDD<BitSet, Integer> columnAttrMaxCount = columnAttrCount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer a, Integer b) {
						return Math.max(a, b);
					}
				});

		// filter non-unique columns
		JavaPairRDD<BitSet, Integer> nonUnique = columnAttrMaxCount
				.filter(new Function<Tuple2<BitSet, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<BitSet, Integer> v1)
							throws Exception {
						if (v1._2() != 1) {
							return true;
						}
						return false;
					}
				});

		// nonUnique columnIndex -> maxAttrCount

		if (DEBUG)
			System.out.println("non uniques: " + nonUnique.collect());

		List<Tuple2<BitSet, Integer>> list = nonUnique.collect();
		for (Tuple2<BitSet, Integer> t : list) {
			if (DEBUG)
				System.out.println("bitset: " + t._1());
			nonUniqueColumns.add(t._1());
		}

		if (DEBUG)
			System.out.println("non unique columns ( "
					+ nonUniqueColumns.size() + " ): " + nonUniqueColumns);

		// -----------------------------------------------------------------------------------------------
		// 2. save unique column combination

		if (DEBUG)
			System.out.println("results:");

		// print distinct columns
		List<Tuple2<BitSet, Integer>> result = columnAttrMaxCount.collect();
		System.out.println("N " + N);
		for (int i = 0; i < N; i++) {
			if (result.get(i)._2 == 1) {
				if (DEBUG)
					System.out.println(result.get(i)._1 + ", "
							+ result.get(i)._2);
				minUcc.add(result.get(i)._1);
			}
		}

		// -----------------------------------------------------------------------------------------------
		// 3. generate new candidates if non-unique column combination
		// found
		// TODO: only generate those candidates for which a subset is
		// not already unique! -> Done because only nonUnique Columns are used

		// generates ALL second level candidates
		if (DEBUG)
			System.out.println("2nd level candidates:");

		for (int i = 0; i < nonUniqueColumns.size() - 1; i++) {
			for (int j = i + 1; j < nonUniqueColumns.size(); j++) {
				BitSet newCandidate = new BitSet(N);
				newCandidate.set(nonUniqueColumns.get(i).nextSetBit(0));
				newCandidate.set(nonUniqueColumns.get(j).nextSetBit(0));
				candidates.add(newCandidate);
				if (DEBUG)
					System.out.println(newCandidate);
			}
		}

		if (DEBUG) {
			System.out.println(candidates.isEmpty());
		}
	}

	/**
	 * This method reads the given file row by row and create a spark RDD of
	 * Attributes.
	 * 
	 * @param file
	 *            spark file
	 * @return
	 */
	private static JavaRDD<Attribute> createAttributeValues(JavaRDD<String> file) {
		return file.flatMap(new FlatMapFunction<String, Attribute>() {
			private static final long serialVersionUID = 1L;
			int rowIndex = 0;

			public Iterable<Attribute> call(String s) {
				// under the assumption of horizontal partitioning
				// a local row index should work for combining multiple
				// columns

				String[] strValues = s.split(",");
				if (DEBUG)
					System.out.println(s);
				List<Attribute> attributes = new ArrayList<Attribute>();
				for (int i = 0; i < N; i++) {
					BitSet bs = new BitSet(N);
					bs.set(i);
					attributes.add(new Attribute(bs, rowIndex, strValues[i]));
				}
				rowIndex++;
				return attributes;
			}
		});
	}

	/**
	 * This method reads one row to determine number of columns.
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
		@SuppressWarnings("resource")
		BufferedReader buffer = new BufferedReader(reader);
		String firstRow = buffer.readLine();
		// TODO csv reader
		return firstRow.split(",").length;
	}

	static class Attribute implements Serializable {
		private static final long serialVersionUID = 1L;
		BitSet columnIndex;
		int rowIndex;
		// TODO: data type
		String value;

		public Attribute(BitSet columnIndex, int rowIndex, String value) {
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
			Attribute attr = (Attribute) obj;
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