package com.dataprofiling.ucc;

import java.io.BufferedReader;
import java.io.FileReader;
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
 * @author pjung
 *
 */
public class Ucc {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		final boolean debug = false;

		final String inputFile = args[0];
		// final String outputFile = args[1];

		// final String tableIdentifier = "LOD-small_sampled";
		final String tableIdentifier = "WDC_planets";

		SparkConf config = new SparkConf().setAppName("Median");
		config.set("spark.hadoop.validateOutputSpecs", "false");

		JavaSparkContext spark = new JavaSparkContext(config);
		JavaRDD<String> file = spark.textFile(inputFile + "/" + tableIdentifier
				+ ".csv");

		// read on row to determine #column n
		FileReader reader = new FileReader(inputFile + "/" + tableIdentifier
				+ ".csv");
		BufferedReader buffer = new BufferedReader(reader);
		String firstRow = buffer.readLine();
		// TODO csv reader
		System.out.println(firstRow);
		final int n = firstRow.split(",").length;

		// encode column combinations as bit sets
		List<BitSet> candidates = new ArrayList<>();
		Set<BitSet> minUcc = new HashSet<>();

		final List<BitSet> nonUniqueColumns = new ArrayList<BitSet>();

		int levelIndex = 1;

		// generate first level candidates
		// for(int i = 0; i < n; i++) {
		// BitSet candidate = new BitSet(n);
		// candidate.set(i);
		// candidates.add(candidate);
		// }

		// add empty bitset so list is not empty initially
		candidates.add(new BitSet(1));

		// initially read all data once
		// split rows
		JavaRDD<Attribute> attributeValues = file
				.flatMap(new FlatMapFunction<String, Attribute>() {
					int rowIndex = 0;

					public Iterable<Attribute> call(String s) {
						// under the assumption of horizontal partitioning
						// a local row index should work for combining multiple
						// columns

						String[] strValues = s.split(",");
						if (debug) System.out.println(s);
						List<Attribute> attributes = new ArrayList<Attribute>();
						for (int i = 0; i < n; i++) {
							attributes.add(new Attribute(i, rowIndex,
									strValues[i]));
						}
						rowIndex++;
						return attributes;
					}
				});

		if (debug) System.out.println(attributeValues.collect());

		// from bottom to top, check each column combination on given lattice
		// level
		while (!candidates.isEmpty()) {

			if (debug) System.out.println("levelIndex: " + levelIndex);
			// -----------------------------------------------------------------------------------------------
			// 1. check candidates for uniqueness

			final Broadcast<Integer> locallevelIndex = spark
					.broadcast(levelIndex);
			final Broadcast<BitSet> candidate = spark.broadcast(candidates
					.remove(0));
			final BitSet candidateCopy = candidate.value();

			if (debug)
				System.out.println("candidate to check: " + candidateCopy);

			if (locallevelIndex.value() == 1) {
				// do single-column checks
				// dont filter, just check all single columns simultaneously
				// instead

				// map attributes values
				JavaPairRDD<Attribute, Integer> pairs = attributeValues
						.mapToPair(new PairFunction<Attribute, Attribute, Integer>() {

							public Tuple2<Attribute, Integer> call(
									Attribute attr) {
								return new Tuple2<Attribute, Integer>(attr, 1);
							}
						});

				if (debug) {
					Map<Attribute, Object> m = pairs.countByKey();
					for (Attribute a : m.keySet()) {
						System.out.print(a + ": ");
						System.out.println(m.get(a));
					}
					System.out.println(pairs.collect());
				}

				JavaPairRDD<Attribute, Integer> counts = pairs
						.reduceByKey(new Function2<Integer, Integer, Integer>() {

							public Integer call(Integer a, Integer b) {
								return a + b;
							}
						});

				if (debug)
					System.out.println("reduced: " + counts.collect());

				// key = column index, value is sum of occurrences of the same
				// attribute
				// value
				JavaPairRDD<Integer, Integer> columnAttrCount = counts
						.mapToPair(new PairFunction<Tuple2<Attribute, Integer>, Integer, Integer>() {

							public Tuple2<Integer, Integer> call(
									Tuple2<Attribute, Integer> t)
									throws Exception {
								return new Tuple2<Integer, Integer>(
										t._1.columnIndex, t._2);
							}
						});

				if (debug)
					System.out.println(columnAttrCount.collect());

				// key = column index, value = max of all attribute values sums
				JavaPairRDD<Integer, Integer> columnAttrMaxCount = columnAttrCount
						.reduceByKey(new Function2<Integer, Integer, Integer>() {
							private static final long serialVersionUID = 1L;

							public Integer call(Integer a, Integer b) {
								return Math.max(a, b);
							}
						});

				// filter non-unique columns
				JavaPairRDD<Integer, Integer> nonUnique = columnAttrMaxCount
						.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
							private static final long serialVersionUID = 1L;

							public Boolean call(Tuple2<Integer, Integer> v1)
									throws Exception {
								if (v1._2() != 1) {
									return true;
								}
								return false;
							}
						});

				// nonUnique columnIndex -> maxAttrCount

				if (debug)
					System.out.println("non uniques: " + nonUnique.collect());

				List<Tuple2<Integer, Integer>> list = nonUnique.collect();
				for (Tuple2<Integer, Integer> t : list) {
					BitSet nw = new BitSet(n);
					nw.set(t._1());
					if (debug)
						System.out.println("bitset: " + nw);
					nonUniqueColumns.add(nw);
				}

				if (debug)
					System.out.println("non unique columns ( "
							+ nonUniqueColumns.size() + " ): "
							+ nonUniqueColumns);

				// -----------------------------------------------------------------------------------------------
				// 2. save unique column combination

				if (debug)
					System.out.println("results:");

				// print distinct columns
				List<Tuple2<Integer, Integer>> result = columnAttrMaxCount
						.collect();
				for (int i = 0; i < n; i++) {
					if (result.get(i)._2 == 1) {
						if (debug) System.out.println(result.get(i)._1 + ", "
								+ result.get(i)._2);
						BitSet ucc = new BitSet(n);
						ucc.set(result.get(i)._1);
						minUcc.add(ucc);
					}
				}

				// -----------------------------------------------------------------------------------------------
				// 3. generate new candidates if non-unique column combination
				// found
				// TODO: only generate those candidates for which a subset is
				// not already unique!

				// generates ALL second level candidates
				if (debug)
					System.out.println("2nd level candidates:");

				for (int i = 0; i < nonUniqueColumns.size() - 1; i++) {
					for (int j = i + 1; j < nonUniqueColumns.size(); j++) {
						BitSet newCandidate = new BitSet(n);
						newCandidate.set(nonUniqueColumns.get(i).nextSetBit(0));
						newCandidate.set(nonUniqueColumns.get(j).nextSetBit(0));
						candidates.add(newCandidate);
						if (debug)
							System.out.println(newCandidate);
					}
				}

				levelIndex++;
				if (debug) {
					System.out.println("increased level index");
					System.out.println(candidates.isEmpty());
				}
			} else {
				// do multi-column check
				if (debug)
					System.out.println("do multi column check");

				// only do check if subset is not already unique
				// candidate generation does not check for subset uniqueness
				BitSet toCheck = (BitSet) candidateCopy.clone();
				boolean subsetUnique = false;

				for (BitSet min : minUcc) {
					BitSet clone = (BitSet) min.clone();
					clone.and(toCheck);

					if (clone.cardinality() == min.cardinality()) {
						// subset is already unique
						subsetUnique = true;
					}
				}
				if (!subsetUnique) {

					// filter only values for the column to be checked
					JavaRDD<Attribute> filtered = attributeValues
							.filter(new Function<Ucc.Attribute, Boolean>() {
								// TODO: how expensive is this filter?
								BitSet currentAttribute = new BitSet(n);

								@Override
								public Boolean call(Attribute v1)
										throws Exception {
									currentAttribute.clear();
									currentAttribute.set(v1.columnIndex);

									if (candidateCopy
											.intersects(currentAttribute)) {
										return true;
									}
									return false;
								}
							});

					if (debug)
						System.out.println(filtered.collect());

					JavaPairRDD<Integer, String> row2Value = filtered
							.mapToPair(new PairFunction<Ucc.Attribute, Integer, String>() {

								@Override
								public Tuple2<Integer, String> call(Attribute t)
										throws Exception {
									// row -> value
									return new Tuple2(t.rowIndex, t.value);
								}
							});

					// row -> combined value of multiple columns
					JavaPairRDD<Integer, String> columnCombination = row2Value
							.reduceByKey(new Function2<String, String, String>() {

								@Override
								public String call(String v1, String v2)
										throws Exception {
									return v1 + "_#_" + v2;
								}
							});

					JavaPairRDD<String, Integer> nonUnique = columnCombination
							.mapToPair(
									new PairFunction<Tuple2<Integer, String>, String, Integer>() {

										@Override
										public Tuple2<String, Integer> call(
												Tuple2<Integer, String> t)
												throws Exception {
											return new Tuple2<String, Integer>(
													t._2, 1);
										}
									})
							.reduceByKey(
									new Function2<Integer, Integer, Integer>() {

										@Override
										public Integer call(Integer v1,
												Integer v2) throws Exception {
											return v1 + v2;
										}
									})
							.reduceByKey(
									new Function2<Integer, Integer, Integer>() {

										@Override
										public Integer call(Integer v1,
												Integer v2) throws Exception {
											return Math.max(v1, v2);
										}
									})
							.filter(new Function<Tuple2<String, Integer>, Boolean>() {

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
					if (debug)
						System.out.println(list);

					if (nonUnique.count() == 0) {
						if (debug)
							System.out.println("candidate UNIQUE! : "
									+ candidateCopy);
						minUcc.add(candidateCopy);
					} else {
						if (debug)
							System.out.println("candidate not unique! : "
									+ candidateCopy);
					}

					// generate next level, if column combination not size n yet
					if (candidateCopy.cardinality() < n) {

						Iterator<BitSet> it = nonUniqueColumns.iterator();
						while (it.hasNext()) {
							BitSet nonUniqueColumn = it.next();
							BitSet nonUniqueColumnCopy = (BitSet) nonUniqueColumn
									.clone();
							BitSet candidateCopyClone = (BitSet) candidateCopy
									.clone();

							nonUniqueColumnCopy.and(candidateCopyClone);

							if (nonUniqueColumnCopy.cardinality() == 1) {
								// dont combine because column is already
								// contained
							} else {
								candidateCopyClone.or(nonUniqueColumn);

								// add only if not in candidate list already
								if (!candidates.contains(candidateCopyClone)) {
									// TODO: and if subset is not already unique									
									candidates.add(candidateCopyClone);
								}
							}
						}
					}
				}
			}
		}

		// print all minimal uniques
		System.out.println("Minimal Unique Column Combinations:");
		for (BitSet cc : minUcc) {
			System.out.println(cc);
		}

		spark.close();
	}

	static class Attribute implements Serializable {
		private static final long serialVersionUID = 1L;
		int columnIndex;
		int rowIndex;
		// TODO: data type
		String value;

		public Attribute(int columnIndex, int rowIndex, String value) {
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
			if (attr.columnIndex == this.columnIndex
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