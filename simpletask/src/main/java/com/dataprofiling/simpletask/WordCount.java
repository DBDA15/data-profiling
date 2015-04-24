package com.dataprofiling.simpletask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static Text one = new Text("1");
		private IntWritable word = new IntWritable();

		public void map(Object key, Text line, Context context)
				throws IOException, InterruptedException {
			String[] values = line.toString().split(";");
			for (int i = 0; i < values.length; i++) {
				context.write(new Text(i + ""),
						new IntWritable(Integer.valueOf(values[i])));
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<Integer> temperatureList = new ArrayList<Integer>();
			int median = 0;
			for (IntWritable value : values) {
				temperatureList.add(value.get());
			}
			Collections.sort(temperatureList);
			int size = temperatureList.size();

			if (size % 2 == 0) {
				int half = size / 2;

				median = (temperatureList.get(half) + temperatureList
						.get(half + 1)) / 2;
			} else {
				int half = (size + 1) / 2;
				median = temperatureList.get(half - 1);
			}
			context.write(key, new IntWritable(median));
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		 * final String lineItemFile = args[0]; final String outputFile =
		 * args[1];
		 * 
		 * SparkConf config = new SparkConf().setAppName("Median");
		 * config.set("spark.hadoop.validateOutputSpecs", "false");
		 * 
		 * JavaSparkContext spark = new JavaSparkContext(config);
		 * JavaRDD<String> file = spark.textFile(lineItemFile); JavaRDD<String>
		 * words = file .flatMap(new FlatMapFunction<String, String>() { public
		 * Iterable<String> call(String s) { return Arrays.asList(s.split(";"));
		 * } });
		 * 
		 * Function<String, Object> f = new Function<String, Object>() {
		 * 
		 * public Object call(String s) throws Exception { return s; }
		 * 
		 * }; words.sortBy(f, false, 0);
		 * 
		 * JavaPairRDD<String, Integer> pairs = words .mapToPair(new
		 * PairFunction<String, String, Integer>() { public Tuple2<String,
		 * Integer> call(String i) { return new Tuple2<String, Integer>("1",
		 * Integer .valueOf(i)); } });
		 * 
		 * Function2<Integer, Integer, Integer> reduceFunc = new
		 * Function2<Integer, Integer, Integer>() { public Integer call(Integer
		 * a, Integer x) { return a + x; } };
		 * 
		 * JavaPairRDD<String, Integer> counts = pairs.reduceByKey(reduceFunc);
		 * words.saveAsTextFile(outputFile);
		 */

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}