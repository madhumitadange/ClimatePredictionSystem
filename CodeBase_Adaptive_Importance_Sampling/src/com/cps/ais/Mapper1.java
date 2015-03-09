package com.cps.ais;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text Value, Context context)
			throws IOException, InterruptedException {
		// Path[] files =
		// DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// Path QPath = null;
		// for(Path file: files) {
		// if(file.getName().contains("q.txt")) {
		// QPath = file;
		// break;
		// }
		// }
		Network Q = null;
		List<Map<Integer, Integer>> samples = new ArrayList<Map<Integer, Integer>>();
		try {
			Q = NetworkManager
					.createNetwork("/home//prashant//CPS_Data//FileIP//q.txt");
			for (int i = 0; i < 10; i++) {
				Map<Integer, Integer> sample = NetworkManager.generateSample(Q);
				samples.add(sample);
			}
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		for (Map<Integer, Integer> sample : samples) {
			context.write(Value, new Text(convertSampleToString(sample)));
		}
	}

	public String convertSampleToString(Map<Integer, Integer> sample) {
		String output = "";
		Iterator<Integer> keyItr = sample.keySet().iterator();
		while (keyItr.hasNext()) {
			Integer key = keyItr.next();
			Integer value = sample.get(key);
			output = output + key.intValue() + ":" + value.intValue() + ",";
		}
		output = output.substring(0, output.length() - 1);
		return output;
	}
}
