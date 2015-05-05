package edu.upenn.cis555.pagerank;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		// name on the left and val on the right
		String[] splitTab = val.toString().split("\t");
		String[] splitSpace = splitTab[1].split(" ");
		String person = splitSpace[0]; // current person
		String size = splitSpace[1];   // current friendSize
		String rank = splitSpace[2];   // current rank
		String toThem = splitSpace[3];
		if (!toThem.equals("-")) { // if they have friends, split it by comma
			String[] friends = toThem.split(",");

			// give all of my friends a copy of my values
			for (String s : friends) {
				context.write(new Text(s), new Text(person + " " + size + " " + rank));
			}
		}
		// send my name and my values to the reducer
		context.write(new Text(person), new Text(person + " " + size + " " + rank + " " + toThem));
	}
}
