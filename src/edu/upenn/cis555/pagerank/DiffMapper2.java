package edu.upenn.cis555.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class DiffMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		String line = val.toString();
		String[] lineSplitTab = line.split("\t");
		String difference = lineSplitTab[0]; // the difference
		
		// put all the differences in the same reducer
		context.write(new Text("1"), new Text(difference));
	}
}
