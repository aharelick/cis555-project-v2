package edu.upenn.cis555.pagerank;

import java.io.IOException;



import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffReducer2 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> i = values.iterator();
		double diff = Double.NEGATIVE_INFINITY;  // set to lowest possible
		double temp;
		while (i.hasNext()) {
			// if the difference is bigger than the previously biggest difference
			if ((temp = Double.parseDouble(i.next().toString())) > diff)
				diff = temp;
		}
			
		// write out the biggest difference
		context.write(new Text(diff + ""), new Text(""));
	}
}
