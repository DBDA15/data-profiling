package median;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
 * Distributed Median Computation.
 * Simple implementation: only works for uneven count of numbers, numbers should not repeat.
 *
 */
public class Median {
	
	enum Action {
		DISREGARD_LESS_THAN, DISREGARD_LARGER_THAN, NOTHING
	}
	
	/**
	 * 
	 * algorithm:
	 * 1. master queries nodes for the size of their sets of data
	 * 2. master picks random node and queries it for random value (next median to check)
	 * 3. master broadcasts median to all nodes, nodes then partition data into smaller and equal/greater than median
	 * 4. nodes then report the size of their larger-than (m) partition to the master
	 * 5. 	if sum of m is greater than k, master tells each node to disregard less-than set
	 * 		if sum m is less than k, master tells each node to disregard the larger-than set and update k = k - m
	 * 		if sum of m is exactly k, the algorithm terminates and the median has been found
	 * 6. if algorithm has not terminated before, start over at step 1
	 * 
	 * variables: 
	 * n = overall size of data set (sum of all node's sets)
	 * k = k-largest element, e.g. median = n / 2 
	 * m = (sum of) size of larger-than partitions
	 * 
	 */
	
	public static void main(String[] args) throws Exception {

		final String inputFile = args[0];
		final String outputFile = args[1];
		
		SparkConf config = new SparkConf().setAppName("Median");
		config.set("spark.hadoop.validateOutputSpecs", "false");

		JavaSparkContext spark = new JavaSparkContext(config);
		JavaRDD<String> file = spark.textFile(inputFile);

		// split numbers
		JavaRDD<Integer> values = file
				.flatMap(new FlatMapFunction<String, Integer>() {
					
					public Iterable<Integer> call(String s) {
					
						List<Integer> intValues = new ArrayList<Integer>();
						String[] strValues = s.split(";");
						for (String val : strValues) {
							if (!val.isEmpty()) {
								intValues.add(Integer.valueOf(val));
							}
						}
						return intValues;
					}
				});

		JavaPairRDD<String, Integer> pairs = null;
		long n = values.count();
		long k = (n / 2) + 1; // +1 because random value will be counted into larger-than set
		long m = 0;
		boolean done = false;
		
		while (!done) {
			// System.out.println("VALUES: " + values.collect());
			
			// TODO ask random node for a random value and broadcast it
			List<Integer> sample = values.takeSample(false, 2);
			Random rand = new Random();
		    int randomNum = rand.nextInt(2);
			// NOTE: somehow, from a list of 2 values, takeSample(false, 1) would always take the same!
			final Broadcast<Integer> randomMedian = spark.broadcast(sample.get(randomNum));
			
			// System.out.println("MEDIAN: " + randomMedian.value());
			
			// partition data into smaller and equal/greater partitions
			pairs = values
					.mapToPair(new PairFunction<Integer, String, Integer>() {
						Integer median = randomMedian.value();

						public Tuple2<String, Integer> call(Integer i) {
							if (i >= median) {
								return new Tuple2<String, Integer>(
										"larger_than", i);
							} else {
								return new Tuple2<String, Integer>("less_than",
										i);
							}
						}
					});

			// TODO query nodes for the size of m (size of the larger-than
			// partition)
			// System.out.println("Pairs before disregarding: " + pairs.collect());
			
			Map<String, Object> counts = pairs.countByKey();
			m = (long) counts.get("larger_than");
			
			final Broadcast<Action> whatToDisregard;
			
			if (m > k) {
				// disregard less-than set
				whatToDisregard = spark.broadcast(Action.DISREGARD_LESS_THAN);
			} else if (m < k) {
				// disregard larger-than set 
				whatToDisregard = spark.broadcast(Action.DISREGARD_LARGER_THAN);
				k = k - m;
			} else {
				// print median
				// TODO question: what is done by master node, what is done by
				// slaves? e.g. print outs...
				System.out.println("Median: " + randomMedian.value());
				done = true; // avoid going back into loop
				whatToDisregard = spark.broadcast(Action.NOTHING);
				
				// TODO communicate success
				if (whatToDisregard.value().equals(Action.NOTHING)) {
					System.out.println("exit");
					System.exit(1);
				}
			}

			// disregard partitions
			values = pairs
					.filter(new Function<Tuple2<String, Integer>, Boolean>() {

						public Boolean call(Tuple2<String, Integer> v1)
								throws Exception {
							Integer median = randomMedian.value();

							switch (whatToDisregard.value()) {
								case DISREGARD_LESS_THAN:
									return v1._2() >= median;
								case DISREGARD_LARGER_THAN:
									return v1._2() < median;
								case NOTHING:
									System.out
										.println("Program should never get here.");
									break;
							}
							return false;
						}
					}).values();
			
			//System.out.println("Pairs after disregarding: " +  values.collect());
			
		}
		spark.close();
	}
}