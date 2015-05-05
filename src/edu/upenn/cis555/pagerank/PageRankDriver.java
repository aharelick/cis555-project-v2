package edu.upenn.cis555.pagerank;
import java.io.BufferedReader;


import java.io.InputStreamReader;
import java.net.URI;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankDriver {

	public static void main(String[] args) throws Exception {
		String s;
		// generate random folder not in file system to store temp values
		while (true) {
			if (!(existsDirectory(s = UUID.randomUUID().toString())))
				break;
		}
		
		System.out.println("Author: Alex Harelick (harelick)");
		testInput(args[0], args); // test for input errors
		switch (args[0]) {
		case ("init") : exit(init(args)); break; // run init
		case ("iter") : exit(iter(args)); break; // run iter
		case ("diff") : diff(args,s); exit(diff2(args, s)); break;  // run both diffs
		case ("finish") : exit(finish(args)); break;	// run finish
		case ("composite") : exit(composite(args, s)); break;	// run composite (everything)
		}
	}

	// if there are the wrong number of inputs or the number of 
	// reducers is less than one, write to System.err and exit
	public static void testInput(String s, String[] args) {
		switch (s) {
		case ("init"):
			if (args.length != 4) {
				System.err.println("Incorrect Number of Arguments");
				System.exit(1);
			}
			if (Integer.parseInt(args[3]) < 1) {
				System.err.println("Need Positive Reducer Number");
				System.exit(1);
			}
			break;
		case ("iter"):
			if (args.length != 4) {
				System.err.println("Incorrect Number of Arguments");
				System.exit(1);
			}
			if (Integer.parseInt(args[3]) < 1) {
				System.err.println("Need Positive Reducer Number");
				System.exit(1);
			}
			break;
		case ("diff"):
			if (args.length != 5) {
				System.err.println("Incorrect Number of Arguments");
				System.exit(1);
			}
			if (Integer.parseInt(args[4]) < 1) {
				System.err.println("Need Positive Reducer Number");
				System.exit(1);
			}
			break;
		case ("finish"):
			if (args.length != 4) {
				System.err.println("Incorrect Number of Arguments");
				System.exit(1);
			}
			if (Integer.parseInt(args[3]) < 1) {
				System.err.println("Need Positive Reducer Number");
				System.exit(1);
			}
			break;
		case ("composite"):
			if (args.length != 7) {
				System.err.println("Incorrect Number of Arguments");
				System.exit(1);
			}
			if (Integer.parseInt(args[6]) < 1) {
				System.err.println("Need Positive Reducer Number");
				System.exit(1);
			}
			break;
		}
	}
	
	// exit with the right code depending on the boolean parameter
	public static void exit(boolean b) {
		if (b) 
			System.exit(0);
		else 
			System.exit(1);
	}
	
	public static boolean init(String [] arr) throws Exception {
		deleteDirectory(arr[2]);
		// create job
		Job job = new Job();
		job.setJarByClass(PageRankDriver.class);
		// set input/output
		FileInputFormat.addInputPath(job, new Path(arr[1]));
		FileOutputFormat.setOutputPath(job, new Path(arr[2]));
		// set the number of reducers
		job.setNumReduceTasks(Integer.parseInt(arr[3]));
		// set the mapper classes
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);
		// set the output classes for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the output classes for map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true))
			return true;
		return false;
	}

	public static boolean iter(String [] arr) throws Exception {
		deleteDirectory(arr[2]);
		// create job
		Job job = new Job();
		job.setJarByClass(PageRankDriver.class);
		// set input/output
		FileInputFormat.addInputPath(job, new Path(arr[1]));
		FileOutputFormat.setOutputPath(job, new Path(arr[2]));
		// set the number of reducers
		job.setNumReduceTasks(Integer.parseInt(arr[3]));
		// set the mapper classes
		job.setMapperClass(IterMapper.class);
		job.setReducerClass(IterReducer.class);
		// set the output classes for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the output classes for map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if (job.waitForCompletion(true))
			return true;
		return false;
		}
	
	public static void diff(String [] arr, String s) throws Exception {
		deleteDirectory(arr[3]);
		// create job
		Job job = new Job();
		job.setJarByClass(PageRankDriver.class);
		// set input/output
		FileInputFormat.addInputPath(job, new Path(arr[1]));
		FileInputFormat.addInputPath(job, new Path(arr[2]));
		FileOutputFormat.setOutputPath(job, new Path(s));
		// set the number of reducers
		job.setNumReduceTasks(Integer.parseInt(arr[4]));
		// set the mapper classes
		job.setMapperClass(DiffMapper.class);
		job.setReducerClass(DiffReducer.class);
		// set the output classes for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the output classes for map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// if there was an error -> exit
		if (!(job.waitForCompletion(true))) {
			System.exit(1);
		}
		else
			// if not, kill the job and do the next one
			job.killJob();
	}
	
	public static boolean diff2(String [] arr, String s) throws Exception {
		// create job
		Job job = new Job();
		job.setJarByClass(PageRankDriver.class);
		// set input/output
		FileInputFormat.addInputPath(job, new Path(s));
		FileOutputFormat.setOutputPath(job, new Path(arr[3]));
		if (!arr[0].equals("composite")) {
			// set the number of reducers
			job.setNumReduceTasks(Integer.parseInt(arr[4]));
		}
		// set number of reducers
		else {
			job.setNumReduceTasks(1);
		}
		// set the mapper classes
		job.setMapperClass(DiffMapper2.class);
		job.setReducerClass(DiffReducer2.class);
		// set the output classes for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the output classes for map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if (job.waitForCompletion(true)) {
			deleteDirectory(s); // make sure to delete temp directories
			return true;
		}
		else  {
			deleteDirectory(s);  // make sure to delete temp directories
			return false;
		}
	}
	
	public static boolean finish(String[] arr) throws Exception {
		deleteDirectory(arr[2]);
		// create job
		Job job = new Job();
		job.setJarByClass(PageRankDriver.class);
		// set input/output
		FileInputFormat.addInputPath(job, new Path(arr[1]));
		FileOutputFormat.setOutputPath(job, new Path(arr[2]));
		if (!arr[0].equals("composite")) {
			// set the number of reducers
			job.setNumReduceTasks(Integer.parseInt(arr[3]));
		}
		// set number of reducers
		else {
			job.setNumReduceTasks(1);
		}
		// set the mapper classes
		job.setMapperClass(FinishMapper.class);
		job.setReducerClass(FinishReducer.class);
		// set the output classes for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// set the output classes for map
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		if (job.waitForCompletion(true))
			return true;
		return false;
	}
	

	public static boolean composite(String [] arr, String s) throws Exception {
		deleteDirectory(arr[2]);
		// different arrays I pass to the functions based on their inputs
		String [] arrInit = {arr[0], arr[1], arr[3], arr[6]};
		String [] arrIter1 = {arr[0], arr[3], arr[4], arr[6]};
		String [] arrIter2 = {arr[0], arr[4], arr[3], arr[6]};
		String [] arrDiff = {arr[0], arr[3], arr[4], arr[5], arr[6]};
		String [] arrFinish = {arr[0], arr[3], arr[2], arr[6]};
		
		// run init
		if (!(init(arrInit)))
			System.exit(1);
		// run 4 iters and check diff until it's below the limit
		while (true) {
			if (!iter(arrIter1)) 	// run iter
				System.exit(1);	
			if (!iter(arrIter2)) 	// run iter
				System.exit(1);
			if (!iter(arrIter1)) 	// run iter
				System.exit(1);		
			if (!iter(arrIter2)) 	// run iter
				System.exit(1);
			diff(arrDiff, s);	// run diff
			if (!diff2(arrDiff, s)) {// run diff2
				System.exit(1);
			}
			if (readDiffResult(arr[5]) <= 30) //break if it's below the diff
				break;	
		}
		
		
		if (finish(arrFinish))  // run finish
			return true;
		else return false;		
		}

	// Given an output folder, returns the first double from the first
	// part-r-00000 file
	static double readDiffResult(String path) throws Exception {
		double diffnum = 0.0;
		Path diffpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(diffpath)) {
			FileStatus[] ls = fs.listStatus(diffpath);
			for (FileStatus file : ls) {
				if (file.getPath().getName().startsWith("part-r-00000")) {
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(
							new InputStreamReader(diffin));
					String diffcontent = d.readLine();
					diffnum = Double.parseDouble(diffcontent.trim());
					d.close();
				}
			}
		}

		fs.close();
		return diffnum;
	}

	static void deleteDirectory(String path) throws Exception {
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(todelete))
			fs.delete(todelete, true);

		fs.close();
	}
	
	// does the directory exist
	static boolean existsDirectory(String path) throws Exception {
		Path exists = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(exists)) {
			fs.close();
			return true;
		}
		
		else {
			fs.close();
			return false;
		}
	}

}
