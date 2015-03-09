package com.cps.ais;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	Network P = null, Q = null;

	public void setup(Context context) throws IOException, InterruptedException {
		// Path[] files =
		// DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// Path PPath = null, QPath = null;
		// for(Path file: files) {
		// if(file.getName().contains("q.txt")) {
		// QPath = file;
		// } else if(file.getName().contains("p.txt")) {
		// PPath = file;
		// }
		// }
		try {
			P = NetworkManager
					.createNetwork("/home//prashant//CPS_Data//FileIP//p.txt");
			Q = NetworkManager
					.createNetwork("/home//prashant//CPS_Data//FileIP//q.txt");
		} catch (Exception e) {
			throw new IOException(
					"Error in reducer1 while creating P,Q networks."
							+ e.getMessage());
		}
	}

	public void reduce(Text Key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Key = new Text(Key.toString() + "::");
		for (Text sampleText : values) {
			Map<Integer, Integer> sample = convertStringToSample(sampleText
					.toString());
			double wN = NetworkManager.getSampleLikelihood(P, sample);
			double wD = NetworkManager.getSampleLikelihood(Q, sample);
			double w = wN / wD;
			context.write(Key, new Text(sampleText.toString() + "_" + w));
		}
	}

	public Map<Integer, Integer> convertStringToSample(String sampleStr) {
		Map<Integer, Integer> sample = new LinkedHashMap<Integer, Integer>();
		String[] tokens = sampleStr.split(",");
		for (String token : tokens) {
			String[] kv = token.split(":");
			Integer key = Integer.parseInt(kv[0].trim());
			Integer value = Integer.parseInt(kv[1].trim());
			sample.put(key, value);
		}
		return sample;
	}
}
