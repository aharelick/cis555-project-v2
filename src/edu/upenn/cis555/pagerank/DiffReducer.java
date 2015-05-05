package edu.upenn.cis555.pagerank;

import java.io.IOException;


import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> i = values.iterator();	
		double one = 0;
		double two = 0;
		try {
			// first rank
			one = Double.parseDouble(i.next().toString());
		} catch (NoSuchElementException e) {
			one  = 1; // if there's no rank, default to 1
		}
		try {
			// second rank
			two = Double.parseDouble(i.next().toString());
		} catch (NoSuchElementException e) {
			two  = 1; // if there's no rank, default to 1
		}	
			
		// write the absolute value of the difference in ranks
		context.write(new Text(Math.abs(one - two) + ""), new Text(""));
	}
}