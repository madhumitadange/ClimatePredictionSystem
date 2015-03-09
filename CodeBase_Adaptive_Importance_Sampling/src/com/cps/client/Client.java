package com.cps.client;

import java.util.HashMap;

import com.cps.ais.LikelihoodWeighting;
import com.cps.data.UserRequest;
import com.cps.sys.Processor;
import com.google.gson.Gson;

public class Client {

	public Client() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// ZipCode z = new ZipCode();
		// String[] x = new String[3];
		// String inputPath = System.getProperty("user.dir") +
		// "//File//users.dat";
		// String outputPath = System.getProperty("user.dir") + "//OutputFile";
		// x[0] = inputPath;
		// x[1] = outputPath;
		// x[2] = "55455";
		// String status = z.runZipCode(x);
		// System.out.println(status);

		String str = "{\"state\":1,\"yearMonth\":0,\"avgMaxTemp\":0,\"departureMaxTemp\":0,\"avgMinTemp\":0,\"departureMinTemp\":1,\"avgTemp\":0,\"departurefromNormal\":0,\"avgDewPoint\":1,\"avgWetBulb\":0,\"heatingDegreeDays\":0,\"coolingDegreeDays\":0,\"meanStationPressure\":0,\"meanSeaLevelPressure\":1,\"minSeaLevelPressure\":0,\"totalMonthlyPrecip\":0,\"totalSnowfall\":0,\"daysWithPrecip_01\":0,\"daysWithPrecip_10\":0,\"daysWithSnowfall\":0,\"waterEquivalent\":0,\"resultantWindSpeed\":0,\"resultantWindDirection\":0,\"avgWindSpeed\":0}";

		Gson json = new Gson();

		UserRequest request = json.fromJson(str, UserRequest.class);

		System.out.println("Data : " + str + " ...");

		Processor processor = new Processor();
		HashMap<Integer, Integer> map = processor.objectToMap(request);
		
		System.out.println(map.toString());

		LikelihoodWeighting l = new LikelihoodWeighting();
		l.computeLikelihood(map);
	}

}
