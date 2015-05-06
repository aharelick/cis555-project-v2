package edu.upenn.cis555.pagerank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double rank = 1.0;	  // everything starts with rank 1
		String name = key.toString();	// name associated with info
		ArrayList<String> toThem = new ArrayList<String>(); // links I am pointing to

		for (Text t : values) {
			// if they point to me
			toThem.add(t.toString());
		}
		
		String outgoingLinks = "";
		// there are no outgoing links
		if (toThem.size() == 0) {
			outgoingLinks = "-";
		} else {
			// make the friends a comma separated string
			outgoingLinks = StringUtils.join(",", toThem);		
		}
		// String with url, the amount of outgoingLinks, rank, and outgoingLinks
		String output = name + " " + toThem.size() + " " + rank + " " + outgoingLinks;	
		context.write(key, new Text(output));		
	}
}
