package com.cps.ais;

import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

public class CustomComparator implements Comparator<Entry<Text, Double>> {

	public int compare(Entry<Text, Double> o1, Entry<Text, Double> o2) {
		if (o1.getValue() > o2.getValue()) {
			return -1;
		}
		return 1;
	}
}