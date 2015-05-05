package edu.upenn.cis555.pagerank;

import java.io.IOException;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		String line = val.toString();
		String[] splitTab = line.split("\t");
		String[] splitSpace = splitTab[1].split(" ");
		String person = splitTab[0];	// current person
		String rank = splitSpace[2];	// current rank

		// writing as a DoubleWritable will sort the values before reduce
		context.write(new DoubleWritable(-Double.parseDouble(rank)), new Text(person));
	}


}
