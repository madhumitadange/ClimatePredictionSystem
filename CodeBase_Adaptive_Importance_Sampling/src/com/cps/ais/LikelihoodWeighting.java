package com.cps.ais;

import java.util.Map;

public class LikelihoodWeighting {

	@SuppressWarnings("static-access")
	public double computeLikelihood(Map<Integer, Integer> evidence)
			throws Exception {

		double startTime = System.currentTimeMillis();
		String origNetworkFile = "/home//prashant//CPS_Data//FileIP//p.txt";
		String propDistOutputFile = "/home//prashant//CPS_Data//FileIP//q.txt";
		NetworkManager mgr = new NetworkManager();
		Network P = mgr.createNetwork(origNetworkFile);
		Network Q = P.clone(true, -1);
		//mgr.clampEvidence(Q, evidence);
		Q.writeToFile(propDistOutputFile);
		double z1 = mgr.computeLikelihoodWeightingBasedPOE();
		Util.log("LikelihoodWeighting: POE-> z1: " + z1);
		Util.log("LikelihoodWeighting: Total execution time-> "
				+ ((double) (System.currentTimeMillis() - startTime) / 1000)
				+ " secs.");
		return z1;
	}
}
