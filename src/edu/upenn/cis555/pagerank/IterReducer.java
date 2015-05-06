package edu.upenn.cis555.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// values of myself and all the links pointing to me
		String size = null;
		String toThem = null;
		double newRank = 0;
		String source = key.toString();

		for (Text value : values) {
			String[] splitSpace = value.toString().split(" ");
			String link = splitSpace[0];	// link associated with values
			// if I find my own link values
			if (source.equals(link)) {
				// don't use your data in your own rank calculation
				size = splitSpace[1];
				System.out.println(value.toString());
				toThem = splitSpace[3];
			} else {
			// calculate rank
			// index 2: rank, index 1: size
			System.err.println(value.toString());
			newRank = newRank + ((Double.parseDouble(splitSpace[2]) / 
					Double.parseDouble(splitSpace[1])));
			}	
		}

		// finish up rank calc
		newRank = (newRank * (1 - .15)) + .15;

			
		// create output string with the source values
		String write = source + " " + size + " " + newRank + " " + toThem;
		
		context.write(key, new Text(write));

		
	}
}
