package edu.upenn.cis555.pagerank;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		// url on the left and val on the right
		String[] splitTab = val.toString().split("\t");
		String[] splitSpace = splitTab[1].split(" ");
		String source = splitSpace[0];
		String size = splitSpace[1];   // size of outgoing links
		String rank = splitSpace[2];
		String toThem = splitSpace[3];
		if (!toThem.equals("-")) {  
			String[] outgoingLinks = toThem.split(",");

			// give all of my targets a copy of my values
			for (String target : outgoingLinks) {
				context.write(new Text(target), new Text(source + " " + size + " " + rank));
			}
		}
		// send my link and my values to the reducer
		context.write(new Text(source), new Text(source + " " + size + " " + rank + " " + toThem));
	}
}
