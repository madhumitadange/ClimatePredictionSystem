package com.cps.sys;

import java.util.HashMap;

import com.cps.data.UserRequest;

public class Processor {

	public Processor() {
		// TODO Auto-generated constructor stub
	}

	public HashMap<Integer, Integer> objectToMap(UserRequest request) {

		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

		try {

			map.put(0, request.getState());
			map.put(1, request.getYearMonth());
			map.put(2, request.getAvgMaxTemp());
			map.put(3, request.getDepartureMaxTemp());
			map.put(4, request.getAvgMinTemp());
			map.put(5, request.getDepartureMinTemp());
			map.put(6, request.getAvgTemp());
			map.put(7, request.getDeparturefromNormal());
			map.put(8, request.getAvgDewPoint());
			map.put(9, request.getAvgWetBulb());
			map.put(10, request.getHeatingDegreeDays());
			map.put(11, request.getCoolingDegreeDays());
			map.put(12, request.getMeanStationPressure());
			map.put(13, request.getMeanSeaLevelPressure());
			map.put(14, request.getMinSeaLevelPressure());
			map.put(15, request.getTotalMonthlyPrecip());
			map.put(16, request.getTotalSnowfall());
			map.put(17, request.getDaysWithPrecip_01());
			map.put(18, request.getDaysWithPrecip_10());
			map.put(19, request.getDaysWithSnowfall());
			map.put(20, request.getWaterEquivalent());
			map.put(21, request.getResultantWindSpeed());
			map.put(22, request.getResultantWindDirection());
			map.put(23, request.getAvgWindSpeed());

		} catch (Exception e) {

			System.out
					.println("Error occured while parsing the object to map.");
		}
		return map;
	}
}
