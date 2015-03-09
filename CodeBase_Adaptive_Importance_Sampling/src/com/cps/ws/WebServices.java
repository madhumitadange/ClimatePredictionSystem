package com.cps.ws;

import java.util.HashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cps.ais.LikelihoodWeighting;
import com.cps.data.UserRequest;
import com.cps.data.UserResponse;
import com.cps.sys.Processor;
import com.google.gson.Gson;

// The class registers its methods for the HTTP GET request using the @GET annotation. 
// Using the @Produces/@Consumes annotation, it defines that it can deliver/receive several MIME types,
// text, XML and HTML.  

// The browser requests per default the HTML MIME type.

//Sets the path to base URL + /
@Path("/")
public class WebServices {

	// This method is called if TEXT_PLAIN is request
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String sayPlainTextHello() {
		System.out.println("Web Services are hosted and running...");
		return "Welcome to the web services home page of Weather Systems! Created by - prash";
	}

	// This method is called if XML is request
	@GET
	@Produces(MediaType.TEXT_XML)
	public String sayXMLHello() {
		System.out.println("Web Services are hosted and running...");
		return "<?xml version=\"1.0\"?>"
				+ "<hello> Welcome to the web services home page of Weather Systems! Created by - prash </hello>";
	}

	// This method is called if HTML is request
	@GET
	@Produces(MediaType.TEXT_HTML)
	public String sayHtmlHello() {
		System.out.println("Web Services are hosted and running...");
		return "<html> "
				+ "<title>"
				+ "Weather Systems"
				+ "</title>"
				+ "<body><h1>"
				+ "Welcome to the web services home page of Weather Systems! Created by - prash"
				+ "</body></h1>" + "</html> ";
	}

	// http://localhost:8080/ClimatePredictionSystems/ws
	@Path("/ws")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String testWS(String request) {

		System.out.println("Web Services are hosted and running...");
		String response = "Welcome "
				+ request
				+ "to the web services home page of Weather Systems! Created by - prash";

		return response;
	}

	// TODO - get the links
	// http://localhost:8080/ClimatePredictionSystems/backdoor
	@Path("/backdoor")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getUserDetails(String request) {

		Gson json = new Gson();

		UserRequest userRequest = new UserRequest();
		userRequest.setState(0);
		userRequest.setYearMonth(0);
		userRequest.setAvgMaxTemp(0);
		userRequest.setDepartureMaxTemp(0);
		userRequest.setAvgMinTemp(0);
		userRequest.setDepartureMinTemp(0);
		userRequest.setAvgTemp(0);
		userRequest.setDeparturefromNormal(0);
		userRequest.setAvgDewPoint(0);
		userRequest.setAvgWetBulb(0);
		userRequest.setHeatingDegreeDays(0);
		userRequest.setCoolingDegreeDays(0);
		userRequest.setMeanStationPressure(0);
		userRequest.setMeanSeaLevelPressure(0);
		userRequest.setMinSeaLevelPressure(0);
		userRequest.setTotalMonthlyPrecip(0);
		userRequest.setTotalSnowfall(0);
		userRequest.setDaysWithPrecip_01(0);
		userRequest.setDaysWithPrecip_10(0);
		userRequest.setDaysWithSnowfall(0);
		userRequest.setWaterEquivalent(0);
		userRequest.setResultantWindSpeed(0);
		userRequest.setResultantWindDirection(0);
		userRequest.setAvgWindSpeed(0);

		String data = json.toJson(userRequest);

		String response = "http://localhost:8080/ClimatePredictionSystems/uploadData : "
				+ data;

		System.out.println(response);

		return response;
	}

	// TODO - get the links
	// http://localhost:8080/ClimatePredictionSystems/uploadData
	@Path("/uploadData")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public UserResponse uploadData(String jsonRequest) {

		UserResponse response = new UserResponse();
		String status = "";
		try {
			String[] x = {
					"{\"state\":1,\"yearMonth\":0,\"avgMaxTemp\":0,\"departureMaxTemp\":1,\"avgMinTemp\":0,\"departureMinTemp\":0,\"avgTemp\":0,\"departurefromNormal\":0,\"avgDewPoint\":0,\"avgWetBulb\":0,\"heatingDegreeDays\":1,\"coolingDegreeDays\":0,\"meanStationPressure\":0,\"meanSeaLevelPressure\":1,\"minSeaLevelPressure\":0,\"totalMonthlyPrecip\":0,\"totalSnowfall\":0,\"daysWithPrecip_01\":0,\"daysWithPrecip_10\":0,\"daysWithSnowfall\":0,\"waterEquivalent\":0,\"resultantWindSpeed\":0,\"resultantWindDirection\":0,\"avgWindSpeed\":0}",
					"{\"state\":1,\"yearMonth\":0,\"avgMaxTemp\":0,\"departureMaxTemp\":0,\"avgMinTemp\":0,\"departureMinTemp\":1,\"avgTemp\":0,\"departurefromNormal\":0,\"avgDewPoint\":1,\"avgWetBulb\":0,\"heatingDegreeDays\":0,\"coolingDegreeDays\":0,\"meanStationPressure\":0,\"meanSeaLevelPressure\":0,\"minSeaLevelPressure\":0,\"totalMonthlyPrecip\":0,\"totalSnowfall\":1,\"daysWithPrecip_01\":0,\"daysWithPrecip_10\":0,\"daysWithSnowfall\":0,\"waterEquivalent\":0,\"resultantWindSpeed\":0,\"resultantWindDirection\":1,\"avgWindSpeed\":0}",
					"{\"state\":1,\"yearMonth\":0,\"avgMaxTemp\":1,\"departureMaxTemp\":0,\"avgMinTemp\":0,\"departureMinTemp\":0,\"avgTemp\":0,\"departurefromNormal\":0,\"avgDewPoint\":0,\"avgWetBulb\":0,\"heatingDegreeDays\":0,\"coolingDegreeDays\":1,\"meanStationPressure\":0,\"meanSeaLevelPressure\":0,\"minSeaLevelPressure\":0,\"totalMonthlyPrecip\":0,\"totalSnowfall\":0,\"daysWithPrecip_01\":0,\"daysWithPrecip_10\":0,\"daysWithSnowfall\":0,\"waterEquivalent\":0,\"resultantWindSpeed\":1,\"resultantWindDirection\":0,\"avgWindSpeed\":1}" };

			String str = x[randomWithRange(0, 2)];

			jsonRequest = str;

			Gson json = new Gson();

			UserRequest request = json.fromJson(jsonRequest, UserRequest.class);

			System.out.println("Data : " + jsonRequest + " ...");

			Processor processor = new Processor();
			HashMap<Integer, Integer> map = processor.objectToMap(request);

			// call the class and pass map to it

			// ZipCode z = new ZipCode();
			// String[] x = new String[3];

			// String inputPath = System.getProperty("user.dir")
			// + "//File//users.dat";
			// String outputPath = System.getProperty("user.dir") +
			// "//OutputFile";

			// String inputPath = "/home//prashant//users.dat";
			// String outputPath = "/home//prashant//OUTPUT";
			// x[0] = inputPath;
			// x[1] = outputPath;
			// x[2] = "55455";

			// status = z.runZipCode(x);

			LikelihoodWeighting l = new LikelihoodWeighting();
			status = String.valueOf(l.computeLikelihood(map));

			response.setStatus(status);

		} catch (Exception exp) {

			status = "WS - Exception - Failed to process user request!"
					+ exp.getMessage();
			response.setStatus(status);

			// System.out
			// .println("WS - Exception - Failed to process user request!");
			// exp.printStackTrace();
		}
		return response;
	}

	int randomWithRange(int min, int max) {
		int range = (max - min) + 1;
		return (int) (Math.random() * range) + min;
	}
}