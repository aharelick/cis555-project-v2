package edu.upenn.cis555.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		/* one key and and an iterable value set that contains
		 * all the values of people friends with me as well as my values */
		String Minename = null;
		String MinefriendSize = null;
		String MinetoThem = null;
		double rankNew = 0;

		for (Text t : values) {
			String[] splitSpace = t.toString().split(" "); // current value
			String person = key.toString();	// we are calculating the rank for...
			String name = splitSpace[0];	// name associated with values
			// if the current values are for the key gather the fields
			if (person.equals(name)) {
				Minename = name;
				MinefriendSize = splitSpace[1];
				MinetoThem = splitSpace[3];
				// don't use your data in your own rank calculation
			} else {
			// calculate rank
			rankNew = rankNew + ((Double.parseDouble(splitSpace[2]) / 
					Double.parseDouble(splitSpace[1])));
			}	
		}

		// finish up rank calc
		rankNew = (rankNew * (1 - .15)) + .15;

			
		// create output string with name, size of friends , new rank and friends
		String write = Minename + " " + MinefriendSize + " " + rankNew + " "
				+ MinetoThem;
		
		context.write(key, new Text(write));

		
	}
}
