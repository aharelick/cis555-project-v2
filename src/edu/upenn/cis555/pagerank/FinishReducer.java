package edu.upenn.cis555.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinishReducer extends
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	public void reduce(DoubleWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		// re negate the rank and output in order
		for (Text t : values) {
			context.write(t, new DoubleWritable((-key.get())));
		}
        
	}
}
        

