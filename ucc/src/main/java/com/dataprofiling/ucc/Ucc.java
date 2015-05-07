package com.dataprofiling.ucc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
 * Distributed Median Computation. Simple implementation: only works for uneven
 * count of numbers, numbers should not repeat.
 *
 */
public class Ucc {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {

		final String inputFile = args[0];
		// final String outputFile = args[1];

		SparkConf config = new SparkConf().setAppName("Median");
		config.set("spark.hadoop.validateOutputSpecs", "false");

		JavaSparkContext spark = new JavaSparkContext(config);
		JavaRDD<String> file = spark.textFile(inputFile);

		// read on row to determine #column n
		FileReader reader = new FileReader(inputFile + "/LOD-small.csv");
		BufferedReader buffer = new BufferedReader(reader);
		String firstRow = buffer.readLine();
		// TODO csv reader
		final int n = firstRow.split(",").length;

		// split rows
		JavaRDD<Attribute> attributeValues = file
				.flatMap(new FlatMapFunction<String, Attribute>() {
					private static final long serialVersionUID = 1L;

					public Iterable<Attribute> call(String s) {
						String[] strValues = s.split(",");
						List<Attribute> attributes = new ArrayList<Attribute>();
						for (int i = 0; i < n; i++) {
							attributes.add(new Attribute(i, strValues[i]));
						}
						return attributes;
					}
				});

		// map attributes values
		JavaPairRDD<Attribute, Integer> pairs = attributeValues
				.mapToPair(new PairFunction<Attribute, Attribute, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Attribute, Integer> call(Attribute attr) {
						return new Tuple2<Attribute, Integer>(attr, 1);
					}
				});

		// count occurrences
		// TOFIX: Attribute gleichheit bei reduce by key
		JavaPairRDD<Attribute, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});

		// key = column index, value is sum of occurrences of the same attribute
		// value
		JavaPairRDD<Integer, Integer> columnAttrCount = counts
				.mapToPair(new PairFunction<Tuple2<Attribute, Integer>, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, Integer> call(
							Tuple2<Attribute, Integer> t) throws Exception {
						return new Tuple2<Integer, Integer>(t._1.columnIndex,
								t._2);
					}
				});

		// key = column index, value = max of all attribute values sums
		JavaPairRDD<Integer, Integer> columnAttrMaxCount = columnAttrCount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer a, Integer b) {
						return Math.max(a, b);
					}
				});

		// print distinct columns
		List<Tuple2<Integer, Integer>> result = columnAttrMaxCount.collect();
		for (int i = 0; i < n; i++) {
			if (result.get(i)._2 == 1) {
				System.out.println(result.get(i)._1);
			}
		}

		// filter non-unique columns
		JavaPairRDD<Integer, Integer> nonUnique = columnAttrCount
				.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<Integer, Integer> v1)
							throws Exception {
						return v1._2() == 1;
					}
				});

		// TODO: generate candidates

		spark.close();
	}

	static class Attribute implements Serializable {
		private static final long serialVersionUID = 1L;
		int columnIndex;
		// TODO: data type
		String value;

		public Attribute(int columnIndex, String value) {
			super();
			this.columnIndex = columnIndex;
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
			Attribute attr = (Attribute) obj;
			if (attr.columnIndex == this.columnIndex
					&& attr.value.equals(this.value)) {
				return true;
			}
			return false;

		}

	}
}