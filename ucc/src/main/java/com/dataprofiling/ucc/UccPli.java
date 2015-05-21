package com.dataprofiling.ucc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
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
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class UccPli {

	// global ucc state variables
	private static int N = 0;
	// encode column combinations as bit sets
	private static List<BitSet> candidates = new ArrayList<>();
	private static Set<BitSet> minUcc = new HashSet<>();
	private static final List<BitSet> nonUniqueColumns = new ArrayList<BitSet>();

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
		@SuppressWarnings("resource")
		BufferedReader buffer = new BufferedReader(reader);
		String firstRow = buffer.readLine();
		// TODO csv reader
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
		
		JavaPairRDD<BitSet, List<List<Long>>> plisSingleColumns = createPLIs(cellValues);
		System.out.println(plisSingleColumns.collect());
	
		JavaPairRDD<BitSet, List<List<Long>>> plisSingleColumnsCopy = plisSingleColumns;
		JavaPairRDD<Tuple2<BitSet, List<List<Long>>>, Tuple2<BitSet, List<List<Long>>>> combis = plisSingleColumns.cartesian(plisSingleColumnsCopy).filter(new Function<Tuple2<Tuple2<BitSet,List<List<Long>>>,Tuple2<BitSet,List<List<Long>>>>, Boolean>() {
			
			@Override
			public Boolean call(
					Tuple2<Tuple2<BitSet, List<List<Long>>>, Tuple2<BitSet, List<List<Long>>>> v1)
					throws Exception {
				BitSet xor = (BitSet) v1._1._1.clone();
				xor.xor(v1._2._1);
				
				return xor.cardinality() > v1._1._1.cardinality();
			}
		});
		combis.mapToPair(new PairFunction<Tuple2<Tuple2<BitSet,List<List<Long>>>,Tuple2<BitSet,List<List<Long>>>>, BitSet, List<List<Long>>>() {

			@Override
			public Tuple2<BitSet, List<List<Long>>> call(
					Tuple2<Tuple2<BitSet, List<List<Long>>>, Tuple2<BitSet, List<List<Long>>>> t)
					throws Exception {
				List<List<Long>> intersection = new ArrayList<List<Long>>();
				//intersection = t._1._2 intersect
				// TODO: continue
				return null;//new Tuple2<BitSet, List<List<Long>>>(t._1._1.or(t._2._1), intersection);
			}
		});
		
		spark.close();
	}

	private static JavaPairRDD<BitSet, List<List<Long>>> createPLIs(
			JavaRDD<Cell> cellValues) {
		
		JavaPairRDD<Cell, List<Long>> cell2Positions = cellValues.mapToPair(new PairFunction<UccPli.Cell, Cell, List<Long>>() {

			@Override
			public Tuple2<Cell, List<Long>> call(Cell t) throws Exception {
				List<Long> rowIndex = new ArrayList<Long>();
				rowIndex.add(t.rowIndex);
				return new Tuple2<UccPli.Cell, List<Long>>(t, rowIndex);
			}
		}).reduceByKey(new Function2<List<Long>, List<Long>, List<Long>>() {
			
			@Override
			public List<Long> call(List<Long> v1, List<Long> v2) throws Exception {
				v1.addAll(v2);
				return v1;
			}
		}).cache();
		
		JavaPairRDD<BitSet, List<List<Long>>> plisSingleColumns = cell2Positions.filter(new Function<Tuple2<Cell,List<Long>>, Boolean>() {
			
			@Override
			public Boolean call(
					Tuple2<UccPli.Cell, List<Long>> v1)
					throws Exception {
				return v1._2.size() != 1;
			}
		}).mapToPair(new PairFunction<Tuple2<Cell,List<Long>>, BitSet, List<List<Long>>>() {

			@Override
			public Tuple2<BitSet, List<List<Long>>> call(Tuple2<Cell, List<Long>> v1) throws Exception {
				List<List<Long>> listOfRedundantValueLists = new ArrayList<List<Long>>();
				listOfRedundantValueLists.add(v1._2);
				return new Tuple2<BitSet, List<List<Long>>>(v1._1.columnIndex, listOfRedundantValueLists);
			}
		}).reduceByKey(new Function2<List<List<Long>>, List<List<Long>>, List<List<Long>>>() {
			
			@Override
			public List<List<Long>> call(List<List<Long>> v1, List<List<Long>> v2)
					throws Exception {
				v1.addAll(v2);
				return v1;
			}
		});
		
		List<Tuple2<Cell, List<Long>>> uniqueColumns = cell2Positions.filter(new Function<Tuple2<Cell,List<Long>>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<Cell, List<Long>> v1) throws Exception {
				return v1._2.size() == 1;
			}
		}).collect();
		
		// save unique columns
		for(Tuple2<Cell, List<Long>> col : uniqueColumns) {
			minUcc.add(col._1.columnIndex);
		}
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
