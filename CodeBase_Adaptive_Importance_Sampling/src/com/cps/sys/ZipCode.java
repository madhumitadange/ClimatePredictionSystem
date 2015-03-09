package com.cps.sys;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ZipCode {

	// Global Variable
	public static String zipcode;

	// Mapper
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		// Set the variables
		private Text zipCode = new Text();
		private Text userIDs = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Get the value
			String line = value.toString();

			if (line != null) {

				// Split line
				String[] st = line.split("::");

				// Set and get zipCode and userIDs
				if (zipcode.equalsIgnoreCase(st[4])) {

					zipCode.set(st[4]);
					userIDs.set(st[0]);

					context.write(zipCode, userIDs);
				}
			}
		}
	}

	// Reducer
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text res = new Text();
		private Text mapperKey = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String userID = "";
			int counter = 0;

			for (Text value : values) {

				userID += "\nUserID: " + value;
				counter++;
			}

			res.set(userID + "\n");

			mapperKey.set("\n------------------------------------------"
					+ "\nGiven ZipCode : " + key + "\nNumber of Users found : "
					+ counter + " entries"
					+ "\n------------------------------------------");

			// Write (key, result) out
			context.write(mapperKey, res);
		}
	}

	// Main Program
	public String runZipCode(String[] args) throws Exception {

		String status = "";
		Configuration conf = new Configuration();
		String[] processedArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Check arguments
		if (processedArgs.length != 3) {
			System.err
					.println("To execute the program use the following format: ZipCode <in> <out> <zipcode>");
			status = "Argument Check failed";
			// System.exit(2);
		}

		// Assign zipcode and inPath
		zipcode = processedArgs[2];

		// FilesSystem check
		FileSystem hdfs = FileSystem.get(conf);
		Path outputDir = new Path(processedArgs[1]);

		// Delete the directories if they exist
		if (hdfs.exists(outputDir)) {
			
			hdfs.delete(outputDir, true);
		}

		// Create a job with name 'zipcode'
		Job job = new Job(conf);
		job.setJarByClass(ZipCode.class);
		job.setJobName("zipcode");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Combiner
		// No combiner needed

		// Set output key type
		job.setOutputKeyClass(Text.class);
		// Set output value type
		job.setOutputValueClass(Text.class);

		// Set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(processedArgs[0]));
		// Set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(processedArgs[1]));

		// Wait till the completion of the job
		System.out
				.println(job.waitForCompletion(true) ? "Completed Successfully..."
						: "Failed to Execute!");

		status = job.waitForCompletion(true) ? "X" : "Y";

		return status;
	}
}
