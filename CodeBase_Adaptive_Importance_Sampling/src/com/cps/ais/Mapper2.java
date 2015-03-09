package com.cps.ais;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, NullWritable, Text> {

	 public void map(LongWritable key, Text Value, Context context) throws
	 IOException, InterruptedException {
	 String str = Value.toString();
	 String sample_w = str.split("::")[1];
	 context.write(NullWritable.get(), new Text(sample_w));
	 }

}
