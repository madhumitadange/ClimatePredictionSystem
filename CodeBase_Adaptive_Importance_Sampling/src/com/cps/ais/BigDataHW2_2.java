package com.cps.ais;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigDataHW2_2 {

	public static class AverageRatingMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text Value, Context context)
				throws IOException, InterruptedException {
			String text = Value.toString();
			if (!text.isEmpty()) {
				String[] inpArray = text.trim().split("::");
				Text movie = new Text(inpArray[1].trim());
				double rating = Double.parseDouble(inpArray[2].trim());
				context.write(movie, new DoubleWritable(rating));
			}
		}
	}

	public static class AverageRatingReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text Key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Key = new Text(Key.toString() + "::");
			context.write(Key,
					new DoubleWritable((double) sum / (double) count));
		}
	}

	public static class TopRatingMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		Map<Text, Double> ratingMap = new TreeMap<Text, Double>();

		public void map(LongWritable key, Text Value, Context context)
				throws IOException, InterruptedException {
			String text = Value.toString();
			String inpArray[] = text.split("::");
			Text movie = new Text(inpArray[0].trim());
			double avg = Double.parseDouble(inpArray[1].trim());
			ratingMap.put(movie, avg);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			TreeSet<Entry<Text, Double>> set = new TreeSet<Entry<Text, Double>>(
					new CustomComparator());
			set.addAll(ratingMap.entrySet());
			int cnt = 0;
			for (Map.Entry<Text, Double> entry : set) {
				if (cnt < 10) {
					context.write(NullWritable.get(), new Text(entry.getKey()
							+ "::" + entry.getValue().toString()));
					cnt++;
				} else {
					break;
				}
			}
		}
	}

	public static class TopRatingReducer extends
			Reducer<NullWritable, Text, Text, DoubleWritable> {

		Map<Text, Double> ratingMap = new TreeMap<Text, Double>();
		Map<String, String> movieDetails = new HashMap<String, String>();

		public void setup(Context context) throws IOException,
				InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			BufferedReader br = new BufferedReader(new FileReader(new File(
					files[0].getName())));
			String str = null;
			while ((str = br.readLine()) != null) {
				String[] line = str.split("::");
				movieDetails.put(line[0].trim(), line[1].trim());
			}
			br.close();
		}

		public void reduce(NullWritable Key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				Text val = itr.next();
				String tokens[] = val.toString().trim().split("::");
				ratingMap.put(new Text(tokens[0].trim()),
						Double.parseDouble(tokens[1].trim()));
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {

			TreeSet<Entry<Text, Double>> set = new TreeSet<Entry<Text, Double>>(
					new CustomComparator());
			set.addAll(ratingMap.entrySet());
			int ctr = 0;
			for (Map.Entry<Text, Double> entry : set) {
				if (ctr < 10) {
					context.write(
							new Text(movieDetails.get(entry.getKey().toString()
									.trim())),
							new DoubleWritable(entry.getValue()));
					ctr++;
				} else {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			System.out
					.println("Need Movies.dat, input, output, intermediate paths in this order.");
		} else {
			Path input = new Path(args[1]);
			Path output = new Path(args[2]);
			Path intermediate = new Path(args[3]);

			Configuration conf = new Configuration();
			DistributedCache.addCacheFile(new URI(args[0]), conf);
			for (int a = 0; a < 10; a++) {
				// Configuration conf = new Configuration();
				if (FileSystem.get(conf).exists(intermediate)) {
					FileSystem.get(conf).delete(intermediate, true);
				}
				if (FileSystem.get(conf).exists(output)) {
					FileSystem.get(conf).delete(output, true);
				}
				Job job1 = new Job(conf, "Job1: Average Ratings");
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(DoubleWritable.class);
				job1.setJarByClass(BigDataHW2_2.class);
				job1.setInputFormatClass(TextInputFormat.class);
				job1.setOutputFormatClass(TextOutputFormat.class);
				job1.setMapperClass(AverageRatingMapper.class);
				job1.setReducerClass(AverageRatingReducer.class);
				// if (FileSystem.get(conf).exists(input)) {
				FileInputFormat.addInputPath(job1, input);
				// } else {
				// System.out.println("Input file not present");
				// System.exit(0);
				// }
				FileOutputFormat.setOutputPath(job1, intermediate);
				int code = job1.waitForCompletion(true) ? 0 : 1;

				if (code == 0) {
					// Configuration conf2 = new Configuration();
					Job job2 = new Job(conf, "Job2: TopTen Ratings");
					job2.setJarByClass(BigDataHW2_2.class);
					job2.setOutputKeyClass(NullWritable.class);
					job2.setOutputValueClass(Text.class);
					job2.setInputFormatClass(TextInputFormat.class);
					job2.setOutputFormatClass(TextOutputFormat.class);
					// MultipleInputs.addInputPath(job2, intermediate,
					// TextInputFormat.class, TopRatingMapper.class);
					// MultipleInputs.addInputPath(job2, new Path(args[0]),
					// TextInputFormat.class, MovieDetailsMapper.class);
					job2.setMapperClass(TopRatingMapper.class);
					job2.setReducerClass(TopRatingReducer.class);
					FileInputFormat.addInputPath(job2, intermediate);
					FileOutputFormat.setOutputPath(job2, output);
					code = job2.waitForCompletion(true) ? 0 : 1;
				}
			}
		}
	}
}