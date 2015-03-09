package com.cps.ais;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Util {
	private static String logFileName = "util.log";
	@SuppressWarnings("unused")
	private static BufferedWriter bw;
	static {
		try {
			bw = new BufferedWriter(new FileWriter(logFileName));
		} catch (Exception e) {
			System.out
					.println("Util: Error while trying to open writer to write to util.log.");
		}
	}

	public static void log(String msg) throws Exception {
		System.out.println(msg);
		// bw.write(msg);
		// bw.newLine();
		// bw.flush();
	}
}
