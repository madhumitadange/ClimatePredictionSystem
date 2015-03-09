package com.cps.ais;

import java.util.Iterator;
import java.util.Map;

public class ParallelWorkerThread extends Thread {

	public int id;
	public static Network P;
	public static Network Q;
	public static Network Qtemp;
	public static double z = 0;
	public static int I = 0;

	public ParallelWorkerThread() {
	}

	@SuppressWarnings("static-access")
	public void run() {
		// System.out.println("Thread"+id+" execution started");

		try {
			double zLocal = 0;
			Network qLocal = Qtemp.clone(false, 0);

			NetworkManager mgr = new NetworkManager();
			for (int j = 0; j < I; j++) {
				Map<Integer, Integer> sample = mgr.generateSample(Q);
				double wN = mgr.getSampleLikelihood(P, sample);
				double wD = mgr.getSampleLikelihood(Q, sample);
				double w = wN / wD;
				zLocal = zLocal + w;
				Iterator<Function> itr = qLocal.functions.values().iterator();
				while (itr.hasNext()) {
					Function f = itr.next();
					Object values = f.values;
					for (int k = 0; k < f.variables.size() - 1; k++) {
						values = ((Object[]) values)[sample.get(f.variables
								.get(k).index)];
					}
					((Object[]) values)[sample.get(f.variables.get(f.variables
							.size() - 1).index)] = (Double) ((Object[]) values)[sample
							.get(f.variables.get(f.variables.size() - 1).index)]
							+ w;
				}
			}
			finished(qLocal, zLocal);
		} catch (Exception e) {
			System.out.println("Error in execution of thread-" + id + ". "
					+ e.getMessage());
		}

		// System.out.println("Thread"+id+" execution finished");
	}

	private synchronized void finished(Network qLocal, double zLocal)
			throws Exception {
		ParallelWorkerThread.z = ParallelWorkerThread.z + zLocal;
		Iterator<Integer> itr = ParallelWorkerThread.Qtemp.functions.keySet()
				.iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			Function fTemp = ParallelWorkerThread.Qtemp.functions.get(key);
			Function fLocal = qLocal.functions.get(key);
			fTemp.values = fTemp.add(fTemp.values, fLocal.values);
		}
	}

}
