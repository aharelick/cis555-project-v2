package edu.upenn.cis555.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;


public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String line = val.toString();
		// tab separated line input for url to url mappings
		
		String [] tokens = line.split("\t");
		if(tokens.length <= 1) {
			System.out.println("skipping badly formatted line: " + line);
			return;
		}
		
		String source = tokens[0];
		String listOfUrls = tokens[1];
		String bracketsRemoved = listOfUrls.substring(listOfUrls.indexOf('[') + 1,
				listOfUrls.lastIndexOf(']'));
		
		if(bracketsRemoved.isEmpty()) {
			System.out.println("skipping this line. no list of urls for the url: " + source);
			return;
		}
		
		String [] allUrls = bracketsRemoved.split(",");
		for (String target : allUrls) {
			// shouldn't count a self link
			if (!(source.equals(target))) {
				context.write(new Text(source), new Text(target));
			}
			// shouldn't count a self link
			if (!(source.equals(target))) {
				context.write(new Text(target), new Text("X " + source));
			}	
		}
	} 
} 

