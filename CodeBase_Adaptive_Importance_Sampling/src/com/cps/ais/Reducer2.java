package com.cps.ais;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<NullWritable, Text, Text, Text> {

	// Path QPath = null;
	Network Q = null;
	Network Qtemp = null;
	double z = 0;

	public void setup(Context context) throws IOException, InterruptedException {
		// Path[] files =
		// DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// for(Path file: files) {
		// if(file.getName().contains("q.txt")) {
		// QPath = file;
		// }
		// }
		try {
			Q = NetworkManager
					.createNetwork("/home//prashant//CPS_Data//FileIP//q.txt");
			Qtemp = Q.clone(false, 0);
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}

	public void reduce(NullWritable Key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			Text val = itr.next();
			String tokens[] = val.toString().split("_");
			String sampleText = tokens[0];
			double w = Double.parseDouble(tokens[1]);
			z = z + w;
			Map<Integer, Integer> sample = convertStringToSample(sampleText);
			Iterator<Function> fitr = Qtemp.functions.values().iterator();
			while (fitr.hasNext()) {
				Function f = fitr.next();
				Object fvalues = f.values;
				for (int k = 0; k < f.variables.size() - 1; k++) {
					fvalues = ((Object[]) fvalues)[sample.get(f.variables
							.get(k).index)];
				}
				((Object[]) fvalues)[sample.get(f.variables.get(f.variables
						.size() - 1).index)] = (Double) ((Object[]) fvalues)[sample
						.get(f.variables.get(f.variables.size() - 1).index)]
						+ w;
			}
		}
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {

		context.write(new Text(""), new Text(z + ""));

		Iterator<Function> itr = Qtemp.functions.values().iterator();
		while (itr.hasNext()) {
			Function f = itr.next();
			f.normalizeParameters(f.values, 0);
		}

		Iterator<Integer> keyItr = Qtemp.functions.keySet().iterator();
		while (keyItr.hasNext()) {
			int key = keyItr.next();
			Function fTemp = Qtemp.functions.get(key);
			Function f = Q.functions.get(key);
			f.updateParametersAdaptiveImportance(f.values, fTemp.values, 0.2, 0);
		}

		try {
			Q.writeToFile(context);
		} catch (Exception e) {
			System.out.println(e.getMessage());
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
