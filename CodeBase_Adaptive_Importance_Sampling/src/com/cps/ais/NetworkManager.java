package com.cps.ais;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NetworkManager {

	private static Random rand;
	private long[] seeds = { 251l, 2532987654l, 5009893265l, 7546372819l,
			9984232187l };

	public NetworkManager() {
		rand = new Random(seeds[(int) Math.floor(Math.random() * seeds.length)]);
	}

	public static Network createNetwork(String networkFilePath)
			throws Exception {
		Network network = new Network();
		BufferedReader br = new BufferedReader(new FileReader(networkFilePath));
		try {
			String str = br.readLine();
			str = str.trim();
			if ("MARKOV".equalsIgnoreCase(str))
				network.type = NetworkType.MARKOV;
			else if ("BAYES".equalsIgnoreCase(str))
				network.type = NetworkType.BAYES;

			str = br.readLine();
			str = str.trim();
			int varCnt = Integer.parseInt(str.trim());
			network.variables = new Variable[varCnt];

			str = br.readLine();
			str = str.trim();
			String[] domainsizes = str.split("\\s+");
			Variable v = null;
			for (int i = 0; i < domainsizes.length; i++) {
				v = new Variable(i);
				v.domainSize = Integer.parseInt(domainsizes[i].trim());
				network.variables[i] = v;
			}

			str = br.readLine();
			str = str.trim();
			int fnCnt = Integer.parseInt(str.trim());
			network.functions = new LinkedHashMap<Integer, Function>();

			network.fnSequence = 0;
			Function f = null;
			while (network.fnSequence < fnCnt) {
				str = br.readLine();
				str = str.trim();
				if (str.length() == 0)
					continue;
				String[] fnDefn = str.split("\\s+");
				f = new Function();
				int fnScopeSize = Integer.parseInt(fnDefn[0].trim());
				List<Integer> fnVariables = Function
						.getFnVariablesFromFnDefn(fnDefn);
				if (fnScopeSize != fnVariables.size()) {
					throw new Exception(
							"Function size is not matching with number of variables specified against it.");
				}
				for (int varIndex : fnVariables) {
					for (int varIndex2 : fnVariables) {
						network.variables[varIndex].connections
								.add(network.variables[varIndex2]);
					}
					network.variables[varIndex].functionRefs
							.add(network.fnSequence);
					f.variables.add(network.variables[varIndex]);
				}
				network.functions.put(network.fnSequence, f);
				network.fnSequence++;
			}

			int fnIndex = 0;
			while ((str = br.readLine()) != null) {
				str = str.trim();
				if (str.length() == 0)
					continue;

				int numOfVals = Integer.parseInt(str);
				f = network.functions.get(fnIndex);

				int i = 0;
				String[] fnVals = new String[numOfVals];
				while (i < numOfVals) {
					str = br.readLine();
					str = str.trim();
					String[] tempVals = str.split("\\s+");
					for (int j = 0; j < tempVals.length; j++) {
						fnVals[i] = tempVals[j].trim();
						i++;
					}
				}
				f.values = new Object[network.variables[f.variables.get(0).index].domainSize];
				network.populateFunction(f, 0, f.values, fnVals, 0,
						fnVals.length);

				fnIndex++;
			}
		} catch (Exception e) {
			throw e;
		} finally {
			br.close();
		}
		return network;
	}

	/**
	 * Assumption: This method assumes that the last variable of every function
	 * denotes the variable to which the respective function(CPT) belongs. This
	 * assumption will hold true in all bayesian networks except the case of a
	 * network where some variables are already instantiated.
	 * 
	 * @param network
	 * @param evidenceFilePath
	 * @throws Exception
	 */
	public void clampEvidence(Network network, Map<Integer, Integer> evidence)
			throws Exception {
		Iterator<Integer> evidItr = evidence.keySet().iterator();
		while (evidItr.hasNext()) {
			int vIndex = evidItr.next();
			int vValue = evidence.get(new Integer(vIndex));
			network.evidence.put(vIndex, vValue);
			network.variables[vIndex].isEvidence = true;
			network.variables[vIndex].value = vValue;
			Integer VIndex = new Integer(vIndex);
			Iterator<Integer> fItr = network.variables[vIndex].functionRefs
					.iterator();
			while (fItr.hasNext()) {
				int fIndex = fItr.next();
				Integer FIndex = new Integer(fIndex);
				Function f = network.functions.get(fIndex);
				if (f.variables.get(f.variables.size() - 1).index == VIndex) {
					Iterator<Variable> vItr = f.variables.iterator();
					while (vItr.hasNext()) {
						Variable v = vItr.next();
						if (v.index != vIndex) {
							network.variables[v.index].functionRefs
									.remove(FIndex);
						}
					}
					f.variables = new ArrayList<Variable>();
					f.variables.add(network.variables[VIndex]);
					f.values = new Object[network.variables[vIndex].domainSize];
					for (int j = 0; j < network.variables[vIndex].domainSize; j++) {
						if (j == vValue) {
							((Object[]) f.values)[j] = new Double(1);
						} else {
							((Object[]) f.values)[j] = new Double(0);
						}
					}
				}
			}
			// TODO: Ideally, should have removed all the connections to
			// variables that are connected only part of this CPT. But, the
			// usecase in which this construction is being called, doesn't
			// depend on connections. Hence, ignoring it for now.
			// network.variables[vIndex].connections.clear();
			// for(int j = 0; j < network.variables.length; j++) {
			// network.variables[j].connections.remove(VIndex);
			// }
		}
	}

	@SuppressWarnings({ "unused", "resource" })
	public double computeLikelihoodWeightingBasedPOE() throws Exception {

		double z = 0;

		Configuration conf = new Configuration();
		// String inputFileOnLinux = "/home/user/Desktop/BigData/input.txt";
		// Path input = new Path("/input");
		// Path inputFile = new Path("/input/input.txt");
		// Path output = new Path("/output");
		// Path intermediate = new Path("/intermediate");

		Path intermediate = new Path("/home//prashant//CPS_Data//FileINT");

		// Path PPath = new Path("/input/p.txt");
		// Path QPath = new Path("/input/q.txt");

		// BufferedWriter bw = new BufferedWriter(new
		// FileWriter(inputFileOnLinux));
		// for(int i = 0; i < 20; i++) {
		// bw.write(""+(i+1));
		// bw.newLine();
		// }
		// bw.flush();
		// bw.close();

		// if(!FileSystem.get(conf).exists(inputFile)) {
		// FileSystem.get(conf).copyFromLocalFile(new
		// Path("/home/user/Desktop/BigData/input.txt"), input);
		// }
		// FileSystem.get(conf).copyFromLocalFile(new
		// Path("/home/user/Desktop/BigData/p.txt"), PPath);
		// FileSystem.get(conf).copyFromLocalFile(new
		// Path("/home/user/Desktop/BigData/q.txt"), QPath);

		for (int i = 0; i < 10; i++) {
			if (i > 0) {
				correctQFile();
			}
			// URI[] cacheFiles = {new URI("/input/p.txt"), new
			// URI("/input/q.txt")};
			// URI[] cacheFiles = new URI[1];
			// cacheFiles[0] = new
			// URI(System.getProperty("user.dir")+"//r.txt");
			// DistributedCache.setCacheFiles(cacheFiles, conf);

			if (FileSystem.get(conf).exists(intermediate)) {
				FileSystem.get(conf).delete(intermediate, true);
			}
			Job job1 = new Job(conf, "Job1: Sampling & Likelihood Weighting");
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setJarByClass(NetworkManager.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			job1.setMapperClass(Mapper1.class);
			job1.setReducerClass(Reducer1.class);
			FileInputFormat.addInputPath(job1, new Path(
					"/home//prashant//CPS_Data//FileIP//input.txt"));
			FileOutputFormat.setOutputPath(job1, intermediate);
			int code = job1.waitForCompletion(true) ? 0 : 1;

			if (code == 0) {

				if (FileSystem.get(conf).exists(
						new Path("/home//prashant//CPS_Data//FileOP"))) {
					FileSystem.get(conf)
							.delete(new Path(
									"/home//prashant//CPS_Data//FileOP"), true);
				}
				Job job2 = new Job(conf, "Job2: Proposal Distribution Updation");
				job2.setOutputKeyClass(NullWritable.class);
				job2.setOutputValueClass(Text.class);
				job2.setJarByClass(NetworkManager.class);
				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);
				job2.setMapperClass(Mapper2.class);
				job2.setReducerClass(Reducer2.class);
				FileInputFormat.addInputPath(job2, intermediate);
				FileOutputFormat.setOutputPath(job2, new Path(
						"/home//prashant//CPS_Data//FileOP"));
				code = job2.waitForCompletion(true) ? 0 : 1;
			}

			FileSystem.get(conf).delete(
					new Path("/home//prashant//CPS_Data//FileIP//q.txt"), true);
			FileSystem
					.get(conf)
					.copyFromLocalFile(
							new Path(
									"/home//prashant//CPS_Data//FileOP//part-r-00000"),
							new Path("/home//prashant//CPS_Data//FileIP//q.txt"));
		}

		BufferedReader br = new BufferedReader(new FileReader(
				"/home//prashant//CPS_Data//FileIP//q.txt"));
		String str = br.readLine();
		return Double.parseDouble(str.trim()) / 250;
	}

	public void correctQFile() throws IOException {
		List<String> qlines = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(
				"/home//prashant//CPS_Data//FileIP//q.txt"));
		String str = "";
		while ((str = br.readLine()) != null) {
			qlines.add(str);
		}
		br.close();

		BufferedWriter bw = new BufferedWriter(new FileWriter(
				"/home//prashant//CPS_Data//FileIP//q.txt"));
		for (int i = 1; i < qlines.size(); i++) {
			bw.write(qlines.get(i));
			bw.newLine();
		}
		bw.flush();
		bw.close();
	}

	public static double getSampleLikelihood(Network network,
			Map<Integer, Integer> sample) {
		double z = 1;
		Iterator<Function> itr = network.functions.values().iterator();
		while (itr.hasNext()) {
			Function f = itr.next();
			Object values = f.values;
			for (int i = 0; i < f.variables.size() - 1; i++) {
				values = ((Object[]) values)[sample
						.get(f.variables.get(i).index)];
			}
			z = z
					* (Double) ((Object[]) values)[sample.get(f.variables
							.get(f.variables.size() - 1).index)];
		}
		return z;
	}

	public static Map<Integer, Integer> generateSample(Network network)
			throws Exception {
		Map<Integer, Integer> sample = new LinkedHashMap<Integer, Integer>();
		List<Integer> functionsProcessed = new ArrayList<Integer>();

		while (functionsProcessed.size() < network.functions.size()) {
			Iterator<Integer> itr = network.functions.keySet().iterator();
			while (itr.hasNext()) {
				Integer key = itr.next();
				if (functionsProcessed.contains(key)) {
					continue;
				}
				Function f = network.functions.get(key);
				boolean isReady = true;
				for (int i = 0; i < f.variables.size() - 1; i++) {
					if (!sample.keySet().contains(f.variables.get(i).index)) {
						isReady = false;
						break;
					}
				}
				if (isReady) {
					Object values = f.values;
					for (int i = 0; i < f.variables.size() - 1; i++) {
						values = ((Object[]) values)[sample.get(f.variables
								.get(i).index)];
					}
					int vValue = -1;
					double prob = NetworkManager.rand.nextDouble();
					double sum = 0;
					for (int j = 0; j < ((Object[]) values).length; j++) {
						sum = sum + (Double) ((Object[]) values)[j];
						if (prob < sum) {
							vValue = j;
							break;
						}
					}
					if (vValue == -1) {
						throw new Exception("Invalid State. Please Check!!!");
					}
					sample.put(f.variables.get(f.variables.size() - 1).index,
							vValue);
					functionsProcessed.add(key);
				}
			}
		}

		return sample;
	}

	public void instantiateEvidence(Network network, String evidenceFilePath)
			throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(evidenceFilePath));
		String str = br.readLine().trim();
		int evidCnt = Integer.parseInt(str);
		int i = 0;
		while (i < evidCnt) {
			str = br.readLine().trim();
			if (str.length() == 0)
				continue;
			String[] tempVals = str.split("[\\s]+");
			int vIndex = Integer.parseInt(tempVals[0]);
			int vValue = Integer.parseInt(tempVals[1]);
			network.evidence.put(vIndex, vValue);
			network.variables[vIndex].isEvidence = true;
			network.variables[vIndex].value = vValue;
			Integer VIndex = new Integer(vIndex);
			Iterator<Integer> fItr = network.variables[vIndex].functionRefs
					.iterator();
			while (fItr.hasNext()) {
				int fIndex = fItr.next();
				Function f = network.functions.get(fIndex);
				f.values = instantiateEvidence(f, f.values, 0, vIndex, vValue);
				f.variables.remove(VIndex);
			}
			network.variables[vIndex].functionRefs.clear();
			network.variables[vIndex].connections.clear();
			for (int j = 0; j < network.variables.length; j++) {
				network.variables[j].connections.remove(VIndex);
			}
			i++;
		}
		br.close();
	}

	public void instantiateEvidence(Network network,
			Map<Integer, Integer> evidence) throws Exception {
		Iterator<Integer> evidItr = evidence.keySet().iterator();
		while (evidItr.hasNext()) {
			int vIndex = evidItr.next();
			int vValue = evidence.get(new Integer(vIndex));
			network.evidence.put(vIndex, vValue);
			network.variables[vIndex].isEvidence = true;
			network.variables[vIndex].value = vValue;
			Integer VIndex = new Integer(vIndex);
			Iterator<Integer> fItr = network.variables[vIndex].functionRefs
					.iterator();
			while (fItr.hasNext()) {
				int fIndex = fItr.next();
				Function f = network.functions.get(fIndex);
				f.values = instantiateEvidence(f, f.values, 0, vIndex, vValue);
				f.variables.remove(VIndex);
			}
			network.variables[vIndex].functionRefs.clear();
			network.variables[vIndex].connections.clear();
			for (int j = 0; j < network.variables.length; j++) {
				network.variables[j].connections.remove(VIndex);
			}
		}
	}

	private Object instantiateEvidence(Function f, Object oldValues,
			int fnVarIndex, int vIndex, int vValue) throws Exception {
		Object newValues = null;
		if (f.variables.get(fnVarIndex).index == vIndex) {
			newValues = ((Object[]) oldValues)[vValue];
		} else {
			newValues = new Object[((Object[]) oldValues).length];
			for (int i = 0; i < ((Object[]) newValues).length; i++) {
				((Object[]) newValues)[i] = instantiateEvidence(f,
						((Object[]) oldValues)[i], fnVarIndex + 1, vIndex,
						vValue);
			}
		}
		return newValues;
	}

	public double computePOE(Network network) throws Exception {
		int vIndex = -1;

		// Util.log("NwMgrImpl.computePOE: Listing all functions details before starting computePOE.");
		Iterator<Integer> itr = network.functions.keySet().iterator();
		StringBuilder str;
		while (itr.hasNext()) {
			int fnIndex = itr.next();
			Function f = network.functions.get(fnIndex);
			str = new StringBuilder();
			str.append("Fn-");
			str.append(fnIndex);
			str.append(", Vars-");
			for (Variable var : f.variables) {
				str.append(var.index);
				str.append(",");
			}
			// Util.log("NwMgrImpl.computePOE: "+str.toString());
		}
		// Util.log("NwMgrImpl.computePOE: Done preliminary listing of all functions.");

		for (int i = 0; i < network.variables.length; i++) {
			vIndex = network.getNextMinDegreeVariableIndex();
			// Util.log("NwMgr computePOE: "+i+"th min degree var: "+vIndex);
			if (vIndex == -1)
				break;
			Variable v = network.variables[vIndex];
			if (v.isSummedOut) {
				throw new Exception(
						"Trying to sum out an already summed out variable.");
			} else {
				v.isSummedOut = true;
			}
			network.productAndSumOut(vIndex);
			// Util.log("NwMgr computePOE: Done with productandsumout of "+i+"th min degree var ("+vIndex+").");
		}

		double poe = 1;
		for (Function fi : network.functions.values()) {
			if (fi.values instanceof Double) {
				// System.out.println("fi.values: "+((Double)fi.values)+"; variables: "+fi.variables.size());
				poe = poe * ((Double) fi.values);
			} else {
				throw new Exception("There is a function with data left.");
			}
		}
		// Util.log("Probability Of Evidence: "+poe);
		return poe;
	}

	// Note: This won't work on instantiated networks, because clone doesn't
	// work.
	public void computeMinDegreeOrdering(Network network) throws Exception {
		Network cNetwork = network.clone(true, -1);

		for (int i = 0; i < cNetwork.variables.length; i++) {
			int minDegree = Integer.MAX_VALUE;
			int minDegreeVar = -1;
			for (int j = 0; j < cNetwork.variables.length; j++) {
				if ((cNetwork.minDegreeOrder.indexOf(new Integer(j)) == -1)
						&& (cNetwork.variables[j].functionRefs.size() > 0)
						&& (cNetwork.variables[j].connections.size() < minDegree)) {
					minDegree = cNetwork.variables[j].connections.size();
					minDegreeVar = j;
				}
			}
			if (minDegreeVar == -1) {
				throw new Exception(
						"All variables ordering should be retrieved in uninstantiated networks. Please check.");
				// break;
			}
			cNetwork.minDegreeOrder.add(minDegreeVar);
			Integer MinDegreeVar = new Integer(minDegreeVar);

			Set<Variable> minDegreeVarConnections = cNetwork.variables[minDegreeVar].connections;
			for (Variable minDegreeVarNbrIndex : minDegreeVarConnections) {
				cNetwork.variables[minDegreeVarNbrIndex.index].connections
						.addAll(minDegreeVarConnections);
				if (minDegreeVarNbrIndex.index != minDegreeVar) {
					cNetwork.variables[minDegreeVarNbrIndex.index].connections
							.remove(MinDegreeVar);
				}
			}
		}

		cNetwork.treeWidth = 0;
		for (int i = 0; i < cNetwork.minDegreeOrder.size(); i++) {
			int varIndex = cNetwork.minDegreeOrder.get(i);
			if (cNetwork.variables[varIndex].connections.size() > cNetwork.treeWidth)
				cNetwork.treeWidth = cNetwork.variables[varIndex].connections
						.size();
		}

		network.treeWidth = cNetwork.treeWidth;
		network.minDegreeOrder = cNetwork.minDegreeOrder;
	}

	public List<Integer> generateWCutSet(Network network, int w)
			throws Exception {
		List<Integer> evidenceVariables = new ArrayList<Integer>();
		// Set<Integer> fnExclSet = new HashSet<Integer>();
		List<Integer> varMinOrder = new ArrayList<Integer>();
		List<Set<Variable>> clusters = new ArrayList<Set<Variable>>();
		int[] varOccCnt = new int[network.variables.length];
		int maxClusterSize = 0;

		for (int i = 0; i < network.variables.length; i++) {
			int minDegree = Integer.MAX_VALUE;
			int minDegreeVar = -1;
			for (int j = 0; j < network.variables.length; j++) {
				if ((!network.variables[j].isEvidence)
						&& (varMinOrder.indexOf(new Integer(j)) == -1)
						&& (network.variables[j].functionRefs.size() > 0)
						&& (network.variables[j].connections.size() < minDegree)) {
					minDegree = network.variables[j].connections.size();
					minDegreeVar = j;
				}
			}
			if (minDegreeVar == -1) {
				break;
			}
			varMinOrder.add(minDegreeVar);
			Integer MinDegreeVar = new Integer(minDegreeVar);

			Set<Variable> minDegreeVarConnections = network.variables[minDegreeVar].connections;
			for (Variable minDegreeVarNbrIndex : minDegreeVarConnections) {
				network.variables[minDegreeVarNbrIndex.index].connections
						.addAll(minDegreeVarConnections);
				if (minDegreeVarNbrIndex.index != minDegreeVar) {
					network.variables[minDegreeVarNbrIndex.index].connections
							.remove(MinDegreeVar);
				}
			}
		}

		// Util.log("min degree order list size: "+varMinOrder.size());
		// Util.log("min degree order:");
		// for(int i = 0; i < varMinOrder.size(); i++) {
		// System.out.print(varMinOrder.get(i)+", ");
		// }
		// Util.log("");

		for (int i = 0; i < varMinOrder.size(); i++) {
			int varIndex = varMinOrder.get(i);
			clusters.add(network.variables[varIndex].connections);
			if (clusters.get(i).size() > maxClusterSize)
				maxClusterSize = clusters.get(i).size();
		}

		// Util.log("clusters sizes: ");
		// for(int i = 0; i < clusters.size(); i++) {
		// System.out.print(clusters.get(i).size()+", ");
		// }
		// Util.log("");

		if (maxClusterSize > (w + 1)) {
			for (int i = 0; i < clusters.size(); i++) {
				Iterator<Variable> itr = clusters.get(i).iterator();
				while (itr.hasNext()) {
					Variable var = itr.next();
					varOccCnt[var.index] = varOccCnt[var.index] + 1;
				}
			}
		}

		while (maxClusterSize > (w + 1)) {
			int maxOccCnt = Integer.MIN_VALUE;
			int maxOccCntVar = -1;
			for (int i = 0; i < varOccCnt.length; i++) {
				if ((!evidenceVariables.contains(new Integer(i)))
						&& (varOccCnt[i] > maxOccCnt)) {
					maxOccCnt = varOccCnt[i];
					maxOccCntVar = i;
				}
			}
			if (maxOccCntVar == -1) {
				throw new Exception("Invalid Scenario. Please chk.");
			}
			evidenceVariables.add(maxOccCntVar);

			// Remove the chosen variable from every cluster.
			Integer MaxOccCntVar = new Integer(maxOccCntVar);
			for (int i = 0; i < clusters.size(); i++) {
				clusters.get(i).remove(MaxOccCntVar);
			}

			// Compute the current maxClusterSize again.
			maxClusterSize = 0;
			for (int i = 0; i < clusters.size(); i++) {
				if (maxClusterSize < clusters.get(i).size()) {
					maxClusterSize = clusters.get(i).size();
				}
			}
		}

		return evidenceVariables;
	}

	@SuppressWarnings("static-access")
	public double computeSamplingBasedPOE(String uaiFile, String evidFile,
			int w, int N, boolean isAdaptive) throws Exception {
		double z = 0;

		Network network = this.createNetwork(uaiFile);
		this.instantiateEvidence(network, evidFile);
		List<Integer> evidenceVariables = this.generateWCutSet(network, w);
		Util.log("wCutset size: " + evidenceVariables.size());
		// Util.log("NwMgrImpl.computeSamplingBasedPOE: After Gen of wCutSet.");

		List<Double[]> evidenceDistributions = new ArrayList<Double[]>();
		List<Double[]> tempEvidenceDistributions = new ArrayList<Double[]>();

		for (int i = 1; i <= N; i++) {
			network = this.createNetwork(uaiFile);
			this.instantiateEvidence(network, evidFile);
			if (i == 1) {
				evidenceDistributions = initializeEvidenceDistributions(
						evidenceVariables, network);
				if (isAdaptive) {
					tempEvidenceDistributions = initializeTempEvidenceDistributions(evidenceDistributions);
				}
			}
			Map<Integer, Integer> evidenceSample = generateEvidenceSample(
					network, evidenceVariables, evidenceDistributions);
			this.instantiateEvidence(network, evidenceSample);
			double wN = this.computePOE(network);
			double wD = sampleProbability(network, evidenceSample,
					evidenceDistributions);
			double wT = wN / wD;
			z = z + wT;
			if (isAdaptive) {
				Iterator<Integer> itr = evidenceSample.values().iterator();
				for (int j = 0; j < tempEvidenceDistributions.size(); j++) {
					int vValue = itr.next();
					tempEvidenceDistributions.get(j)[vValue] = tempEvidenceDistributions
							.get(j)[vValue] + wT;
				}
				if (i % 100 == 0) {
					updateEvidenceDistributions(evidenceDistributions,
							tempEvidenceDistributions);
					tempEvidenceDistributions = initializeTempEvidenceDistributions(evidenceDistributions);
				}
			}
			// Util.log("NwMgrImpl.computeSamplingBasedPOE: Done with "+i+"th iteration.");
		}
		z = z / N;
		// Util.log("POE(using sampling): "+z);
		return z;
	}

	@SuppressWarnings("static-access")
	public double computePartialSamplingBasedPOE(String uaiFile,
			String evidFile, int w, double alpha, int I, int N)
			throws Exception {
		double z = 0;
		Network origNetwork = this.createNetwork(uaiFile);

		Network network = origNetwork.clone(true, -1);
		this.instantiateEvidence(network, evidFile);
		List<Integer> evidenceVariables = this.generateWCutSet(network, w);

		List<Double[]> evidenceDistributions = new ArrayList<Double[]>();
		List<Double[]> tempEvidenceDistributions = new ArrayList<Double[]>();

		network = origNetwork.clone(true, -1);
		this.instantiateEvidence(network, evidFile);
		evidenceDistributions = initializeEvidenceDistributions(
				evidenceVariables, network);
		tempEvidenceDistributions = initializeTempEvidenceDistributions(evidenceDistributions);

		for (int i = 0; i < N + 1; i++) {
			z = 0;
			for (int j = 0; j < I; j++) {
				network = origNetwork.clone(true, -1);
				this.instantiateEvidence(network, evidFile);
				Map<Integer, Integer> evidenceSample = generateEvidenceSample(
						network, evidenceVariables, evidenceDistributions);
				this.instantiateEvidence(network, evidenceSample);
				double wN = this.computePOE(network);
				double wD = sampleProbability(network, evidenceSample,
						evidenceDistributions);
				double wT = wN / wD;
				z = z + wT;
				Iterator<Integer> itr = evidenceSample.values().iterator();
				for (int k = 0; k < tempEvidenceDistributions.size(); k++) {
					int vValue = itr.next();
					tempEvidenceDistributions.get(k)[vValue] = tempEvidenceDistributions
							.get(k)[vValue] + wT;
				}
			}
			updateEvidenceDistributions(evidenceDistributions,
					tempEvidenceDistributions, alpha);
			tempEvidenceDistributions = initializeTempEvidenceDistributions(evidenceDistributions);
		}
		z = z / I;
		return z;
	}

	@SuppressWarnings({ "unused", "static-access" })
	private Map<Variable, Integer> generateSample(Network P, List<Double[]> Q)
			throws Exception {
		Map<Variable, Integer> evidenceSample = new LinkedHashMap<Variable, Integer>();
		int i = 0;
		for (i = 0; i < Q.size(); i++) {
			int vValue = -1;
			double prob = this.rand.nextDouble();
			double sum = 0;
			for (int j = 0; j < Q.get(i).length; j++) {
				sum = sum + Q.get(i)[j];
				if (prob < sum) {
					vValue = j;
					break;
				}
			}
			if (vValue == -1)
				throw new Exception("Invalid State. Please Check!!! vIndex: "
						+ i + "; Prob: " + prob + "; Sum: " + sum);
			evidenceSample.put(P.variables[i], vValue);
		}
		return evidenceSample;
	}

	@SuppressWarnings("unused")
	private void updateIndependentVariableDistributions(List<Double[]> Q,
			List<Double[]> Qtemp, double alpha) {
		for (int i = 0; i < Q.size(); i++) {

			double wT = 0.0;
			for (int j = 0; j < Qtemp.get(i).length; j++) {
				wT = wT + Qtemp.get(i)[j];
			}

			Double[] dist = Q.get(i);
			Double[] tempDist = Qtemp.get(i);
			if (wT > 0) {
				for (int j = 0; j < dist.length; j++) {
					dist[j] = (alpha * tempDist[j] / wT)
							+ ((1 - alpha) * dist[j]);
				}
			}
		}
	}

	@SuppressWarnings("unused")
	private List<Double[]> generateIndependentVariableDistributions(
			Network network, String evidFile) throws Exception {

		BufferedReader br = new BufferedReader(new FileReader(evidFile));
		String str = br.readLine().trim();
		int evidCnt = Integer.parseInt(str);
		int i = 0;
		while (i < evidCnt) {
			str = br.readLine().trim();
			if (str.length() == 0)
				continue;
			String[] tempVals = str.split("[\\s]+");
			int vIndex = Integer.parseInt(tempVals[0]);
			int vValue = Integer.parseInt(tempVals[1]);
			network.evidence.put(vIndex, vValue);
			i++;
		}
		br.close();

		List<Double[]> evidenceDistributions = new ArrayList<Double[]>();
		for (i = 0; i < network.variables.length; i++) {
			int vIndex = i;
			int vDomainSize = network.variables[i].domainSize;
			Double[] dist = new Double[vDomainSize];
			for (int j = 0; j < vDomainSize; j++) {
				if (network.evidence.containsKey(vIndex)) {
					if (network.evidence.get(vIndex).equals(j)) {
						dist[j] = 1.0;
					} else {
						dist[j] = 0.0;
					}
				} else {
					dist[j] = ((double) 1) / ((double) vDomainSize);
				}
			}
			evidenceDistributions.add(dist);
		}
		return evidenceDistributions;
	}

	public double sampleProbability(Map<Variable, Integer> sample,
			List<Double[]> Q) throws Exception {
		double sampleProbability = 1;
		Iterator<Integer> itr = sample.values().iterator();
		for (int i = 0; i < sample.size(); i++) {
			sampleProbability = sampleProbability * Q.get(i)[itr.next()];
		}
		return sampleProbability;
	}

	public double computeSamplingBasedPOE(String args[]) throws Exception {
		String uaiFile = args[0];
		String evidFile = args[1];
		int w = Integer.parseInt(args[2]);
		if (w < 0) {
			throw new Exception("w can't be negative.");
		}
		int N = Integer.parseInt(args[3]);
		if (N < 1) {
			throw new Exception("N can only take a positive integer.");
		}
		boolean isAdaptive = args[4].equals("true") ? true : false;
		// System.out.println("isAdaptive is set to "+isAdaptive);

		return this
				.computeSamplingBasedPOE(uaiFile, evidFile, w, N, isAdaptive);
	}

	private List<Double[]> initializeEvidenceDistributions(
			List<Integer> evidenceVariables, Network network) throws Exception {
		List<Double[]> evidenceDistributions = new ArrayList<Double[]>();
		for (int vIndex : evidenceVariables) {
			int vDomainSize = network.variables[vIndex].domainSize;
			Double[] dist = new Double[vDomainSize];
			for (int i = 0; i < vDomainSize; i++) {
				dist[i] = ((double) 1) / ((double) vDomainSize);
			}
			evidenceDistributions.add(dist);
		}

		return evidenceDistributions;
	}

	private List<Double[]> initializeTempEvidenceDistributions(
			List<Double[]> evidenceDistributions) {
		List<Double[]> tempEvidenceDistributions = new ArrayList<Double[]>();
		for (Double[] dist : evidenceDistributions) {
			Double[] tempDist = new Double[dist.length];
			for (int i = 0; i < dist.length; i++) {
				tempDist[i] = 0.0;
			}
			tempEvidenceDistributions.add(tempDist);
		}

		return tempEvidenceDistributions;
	}

	@SuppressWarnings("static-access")
	public Map<Integer, Integer> generateEvidenceSample(Network network,
			List<Integer> evidenceVariables,
			List<Double[]> evidenceDistributions) throws Exception {
		Map<Integer, Integer> evidenceSample = new LinkedHashMap<Integer, Integer>();
		int i = 0;
		for (i = 0; i < evidenceDistributions.size(); i++) {
			int vValue = -1;
			double prob = this.rand.nextDouble();
			double sum = 0;
			for (int j = 0; j < evidenceDistributions.get(i).length; j++) {
				sum = sum + evidenceDistributions.get(i)[j];
				if (prob <= sum) {
					vValue = j;
					break;
				}
			}
			if (vValue == -1)
				throw new Exception("Invalid State. Please Check!!! vIndex: "
						+ i + "; prob: " + prob + "; sum: " + sum);
			evidenceSample.put(evidenceVariables.get(i), vValue);
		}
		return evidenceSample;
	}

	public double sampleProbability(Network network,
			Map<Integer, Integer> evidenceSample,
			List<Double[]> evidenceDistributions) throws Exception {
		double sampleProbability = 1;
		Iterator<Integer> itr = evidenceSample.values().iterator();
		for (int i = 0; i < evidenceSample.size(); i++) {
			sampleProbability = sampleProbability
					* evidenceDistributions.get(i)[itr.next()];
		}
		return sampleProbability;
	}

	public void updateEvidenceDistributions(
			List<Double[]> evidenceDistributions,
			List<Double[]> tempEvidenceDistributions) throws Exception {
		if (evidenceDistributions.size() > 0) {
			double wT = 0.0;
			for (int i = 0; i < tempEvidenceDistributions.get(0).length; i++) {
				wT = wT + tempEvidenceDistributions.get(0)[i];
			}

			for (int i = 0; i < tempEvidenceDistributions.size(); i++) {
				for (int j = 0; j < tempEvidenceDistributions.get(i).length; j++) {
					evidenceDistributions.get(i)[j] = tempEvidenceDistributions
							.get(i)[j] / wT;
				}
			}
		}
	}

	public void updateEvidenceDistributions(
			List<Double[]> evidenceDistributions,
			List<Double[]> tempEvidenceDistributions, double alpha)
			throws Exception {
		if (evidenceDistributions.size() > 0) {
			for (int i = 0; i < tempEvidenceDistributions.size(); i++) {

				double wT = 0.0;
				for (int j = 0; j < tempEvidenceDistributions.get(i).length; j++) {
					wT = wT + tempEvidenceDistributions.get(i)[j];
				}

				if (wT > 0) {
					for (int j = 0; j < tempEvidenceDistributions.get(i).length; j++) {
						evidenceDistributions.get(i)[j] = (alpha
								* tempEvidenceDistributions.get(i)[j] / wT)
								+ ((1 - alpha) * evidenceDistributions.get(i)[j]);
					}
				}
			}
		}
	}

	@SuppressWarnings("resource")
	public Map<int[], Double> gatherSamplesFromDataSet(String fileName)
			throws Exception {
		Map<int[], Double> sampleDataSet = new LinkedHashMap<int[], Double>();

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String str = br.readLine();
		String[] tokens = str.split("[\\s]+");

		int varCnt = Integer.parseInt(tokens[0]);
		int sampleCnt = Integer.parseInt(tokens[1]);

		for (int i = 0; i < sampleCnt; i++) {
			str = br.readLine();
			if (str.trim().equals("")) {
				i--;
				continue;
			}
			tokens = str.split("[\\s]+");
			if (varCnt != tokens.length) {
				throw new Exception(
						"Var cnt didn't match the number of tokens in the data sample.");
			}
			int[] sample = new int[varCnt];
			for (int j = 0; j < varCnt; j++) {
				if ("?".equals(tokens[j])) {
					sample[j] = -1;
				} else {
					sample[j] = Integer.parseInt(tokens[j]);
				}
			}
			// updateDataSet(sampleDataSet, sample);
			sampleDataSet.put(sample, 1.0);
		}

		// NOT REQUIRED
		// double total = 0;
		// Iterator<Double> itr = sampleDataSet.values().iterator();
		// while(itr.hasNext()) {
		// total = total + itr.next();
		// }
		//
		// Iterator<int[]> itr2 = sampleDataSet.keySet().iterator();
		// while(itr2.hasNext()) {
		// int[] key = itr2.next();
		// double val = sampleDataSet.get(key);
		// sampleDataSet.put(key, val/total);
		// }

		return sampleDataSet;
	}

	@SuppressWarnings("unused")
	private void updateDataSet(Map<int[], Double> sampleDataSet, int[] sample) {
		boolean isFound = false;
		Iterator<int[]> itr = sampleDataSet.keySet().iterator();
		while (itr.hasNext()) {
			int[] key = itr.next();
			if (compare(key, sample)) {
				isFound = true;
				double val = sampleDataSet.get(key);
				sampleDataSet.put(key, val + 1);
				break;
			}
		}
		if (!isFound) {
			sampleDataSet.put(sample, 1.0);
		}
	}

	private boolean compare(int[] s1, int[] s2) {
		boolean isSame = true;
		for (int i = 0; i < s1.length; i++) {
			if (s1[i] != s2[i]) {
				isSame = false;
				break;
			}
		}
		return isSame;
	}

	public void learnNetworkParametersFODMLE(Network network,
			Map<int[], Double> data) throws Exception {
		Iterator<int[]> dataItr = data.keySet().iterator();
		while (dataItr.hasNext()) {
			int[] sample = dataItr.next();
			double value = data.get(sample);
			Iterator<Function> fnItr = network.functions.values().iterator();
			while (fnItr.hasNext()) {
				Function fn = fnItr.next();
				fn.updateSampleValue(fn.values, sample, value, 0);
			}
		}

		Iterator<Function> fnItr = network.functions.values().iterator();
		while (fnItr.hasNext()) {
			Function fn = fnItr.next();
			fn.normalizeParameters(fn.values, 0);
		}
	}

	// Assumption: All variables are binary. The same assumption is made in all
	// method calls made through this.
	public Network learnNetworkStructureChowLiu(Map<int[], Double> data,
			int[] domainSizes) throws Exception {
		Network lrndNetwork = new Network();
		lrndNetwork.type = NetworkType.BAYES;
		int varCnt = data.keySet().iterator().next().length;
		lrndNetwork.variables = new Variable[varCnt];
		Variable v = null;
		for (int i = 0; i < varCnt; i++) {
			v = new Variable(i);
			v.domainSize = domainSizes[i];
			lrndNetwork.variables[i] = v;
		}
		double[][] MI = new double[varCnt][varCnt];
		for (int i = 0; i < varCnt; i++) {
			for (int j = i + 1; j < varCnt; j++) {
				MI[i][j] = computeMI(lrndNetwork.variables[i],
						lrndNetwork.variables[j], data);
			}
		}
		// Assumption: At least 2 variable network.
		Set<Integer> varSet = new HashSet<Integer>();
		lrndNetwork.functions = new LinkedHashMap<Integer, Function>();
		lrndNetwork.fnSequence = 0;
		int root = -1;
		while (lrndNetwork.fnSequence < varCnt) {
			int[] edge = getMaxMIEdge(MI);
			MI[edge[0]][edge[1]] = Double.NEGATIVE_INFINITY;
			if (varSet.contains(new Integer(edge[0]))
					&& varSet.contains(new Integer(edge[1]))) {
				if (lrndNetwork.isPathExists(edge[0], edge[1], "")) { // Change
																		// this
																		// condition
																		// to
																		// identify
																		// if
																		// circle
																		// exists
					continue;
				}
			}
			varSet.add(new Integer(edge[0]));
			varSet.add(new Integer(edge[1]));
			Function f;
			if (lrndNetwork.fnSequence == 0) {
				root = edge[0];
				f = new Function();
				f.variables.add(lrndNetwork.variables[edge[0]]);
				f.initializeParamStructure();
				for (Variable varIndex : f.variables) {
					lrndNetwork.variables[varIndex.index].connections
							.addAll(f.variables);
					lrndNetwork.variables[varIndex.index].functionRefs
							.add(lrndNetwork.fnSequence);
				}
				lrndNetwork.functions.put(lrndNetwork.fnSequence, f);
				lrndNetwork.fnSequence++;
			}
			f = new Function();
			f.variables.add(lrndNetwork.variables[edge[0]]);
			f.variables.add(lrndNetwork.variables[edge[1]]);
			f.initializeParamStructure();
			for (Variable varIndex : f.variables) {
				lrndNetwork.variables[varIndex.index].connections
						.addAll(f.variables);
				lrndNetwork.variables[varIndex.index].functionRefs
						.add(lrndNetwork.fnSequence);
			}
			lrndNetwork.functions.put(lrndNetwork.fnSequence, f);
			lrndNetwork.fnSequence++;
		}
		if (lrndNetwork.fnSequence != varCnt) {
			throw new Exception(
					"Some thing wrong with structure detection. There should have been N functions identified. Please check.");
		}
		lrndNetwork.correctFunctionsVariableOrdering(root,
				new ArrayList<Variable>());

		return lrndNetwork;
	}

	private int[] getMaxMIEdge(double[][] MI) {
		int[] edge = new int[2];
		double maxVal = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < MI.length; i++) {
			for (int j = i + 1; j < MI.length; j++) {
				if (maxVal < MI[i][j]) {
					edge[0] = i;
					edge[1] = j;
					maxVal = MI[i][j];
				}
			}
		}
		return edge;
	}

	public double computeMI(Variable x, Variable u, Map<int[], Double> data)
			throws Exception {
		double mi = 0;
		double pxu[][] = new double[x.domainSize][u.domainSize], px[] = new double[x.domainSize], pu[] = new double[u.domainSize];
		for (int i = 0; i < x.domainSize; i++) {
			for (int j = 0; j < u.domainSize; j++) {
				pxu[i][j] = 1;
			}
		}
		for (int i = 0; i < x.domainSize; i++) {
			px[i] = 1;
		}
		for (int i = 0; i < u.domainSize; i++) {
			pu[i] = 1;
		}
		// pxu[0][0] = 1; pxu[0][1] = 1; pxu[1][0] = 1; pxu[1][1] = 1;
		// px[0] = 1; px[1] = 1; pu[0] = 1; pu[1] = 1;

		Iterator<int[]> itr = data.keySet().iterator();
		while (itr.hasNext()) {
			int[] sample = itr.next();
			double value = data.get(sample);
			pxu[sample[x.index]][sample[u.index]] = pxu[sample[x.index]][sample[u.index]]
					+ value;
			px[sample[x.index]] = px[sample[x.index]] + value;
			pu[sample[u.index]] = pu[sample[u.index]] + value;
		}

		for (int i = 0; i < x.domainSize; i++) {
			for (int j = 0; j < u.domainSize; j++) {
				mi = mi
						+ (pxu[i][j] * Math.log10(pxu[i][j] / (px[i] * pu[j])) / Math
								.log10(2));
			}
		}

		return mi;
	}

	public void learnNetworkParametersPODEM(Network network,
			Map<int[], Double> data) throws Exception {
		Iterator<Function> fnItr = network.functions.values().iterator();
		while (fnItr.hasNext()) {
			Function fn = fnItr.next();
			fn.normalizeParameters(fn.values, 0);
		}

		List<Map<int[], Double>> completeData = generateCompleteDataSet(data);
		for (int i = 0; i < 20; i++) {
			List<Map<int[], Double>> tempCompleteData = new ArrayList<Map<int[], Double>>();
			Iterator<Map<int[], Double>> itr = completeData.iterator();
			while (itr.hasNext()) {
				Map<int[], Double> curData = itr.next();
				Map<int[], Double> tempData = new LinkedHashMap<int[], Double>();
				Iterator<int[]> itr2 = curData.keySet().iterator();
				double total = 0.0;
				while (itr2.hasNext()) {
					int[] sample = itr2.next();
					double value = network.getLikelihood(sample);
					total = total + value;
					tempData.put(sample, value);
				}
				itr2 = curData.keySet().iterator();
				while (itr2.hasNext()) {
					int[] sample = itr2.next();
					tempData.put(sample, tempData.get(sample) / total);
				}
				tempCompleteData.add(tempData);
			}
			completeData = tempCompleteData;

			fnItr = network.functions.values().iterator();
			while (fnItr.hasNext()) {
				Function fn = fnItr.next();
				fn.resetParameters(fn.values, 0);
			}

			Iterator<Map<int[], Double>> completeDataItr = completeData
					.iterator();
			while (completeDataItr.hasNext()) {
				Map<int[], Double> dataEntry = completeDataItr.next();
				Iterator<int[]> dataItr = dataEntry.keySet().iterator();
				while (dataItr.hasNext()) {
					int[] sample = dataItr.next();
					double value = dataEntry.get(sample);
					fnItr = network.functions.values().iterator();
					while (fnItr.hasNext()) {
						Function fn = fnItr.next();
						fn.updateSampleValue(fn.values, sample, value, 0);
					}
				}
			}

			fnItr = network.functions.values().iterator();
			while (fnItr.hasNext()) {
				Function fn = fnItr.next();
				fn.normalizeParameters(fn.values, 0);
			}
		}
	}

	private List<Map<int[], Double>> generateCompleteDataSet(
			Map<int[], Double> data) {
		List<Map<int[], Double>> completeData = new ArrayList<Map<int[], Double>>();
		Iterator<int[]> itr = data.keySet().iterator();
		while (itr.hasNext()) {
			int[] sample = itr.next();
			completeData.add(generateMissingData(sample, -1));
		}
		return completeData;
	}

	private Map<int[], Double> generateMissingData(int[] sample, int pos) {
		Map<int[], Double> data = new LinkedHashMap<int[], Double>();
		int missingDataPos = -1;
		for (int i = pos + 1; i < sample.length; i++) {
			if (sample[i] == -1) {
				missingDataPos = i;
				break;
			}
		}
		if (missingDataPos != -1) {
			int[] sample1 = cloneArray(sample);
			sample1[missingDataPos] = 0;
			data.putAll(generateMissingData(sample1, missingDataPos));
			int[] sample2 = cloneArray(sample);
			sample2[missingDataPos] = 1;
			data.putAll(generateMissingData(sample2, missingDataPos));
		} else {
			data.put(sample, 1.0);
		}
		return data;
	}

	private int[] cloneArray(int[] sample) {
		int[] s1 = new int[sample.length];
		for (int i = 0; i < sample.length; i++) {
			s1[i] = sample[i];
		}
		return s1;
	}

	@SuppressWarnings({ "resource", "unused" })
	public double getLogLikelihoodDifference(Network origNw, Network lrndNw,
			String testFileName) throws Exception {
		double lld = 0.0;
		BufferedReader br = new BufferedReader(new FileReader(testFileName));
		String str = br.readLine();
		String[] tokens = str.split("[\\s]+");
		int varCnt = Integer.parseInt(tokens[0]);
		int sampleCnt = Integer.parseInt(tokens[1]);

		while ((str = br.readLine()) != null) {
			if (str.trim().equals("")) {
				continue;
			}
			tokens = str.split("[\\s]+");
			if (tokens.length != varCnt) {
				throw new Exception(
						"token length and variable count of the sample must match.");
			}
			int[] sample = new int[tokens.length];
			for (int i = 0; i < sample.length; i++) {
				sample[i] = Integer.parseInt(tokens[i]);
			}
			lld = lld
					+ Math.abs(origNw.getLogLikelihood(sample)
							- lrndNw.getLogLikelihood(sample));
		}
		br.close();
		return lld;
	}

}
