package edu.upenn.cis555.crawler;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import edu.upenn.cis555.crawler.storage.DBWrapper;

public class Crawler {
	/**
	 * The main method initializes and runs the crawler. All threads used in
	 * the crawler and all databases are initialized here.
	 */
	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.out.println("Usage: <comma separated start URLs>"
					+ " <path to BDB> <path to S3 log> <max size in MB>");
			System.exit(-1);
		}
		//initialize the static DBWrapper with the given path
		DBWrapper.init(args[1]);

		maxSize = Long.parseLong(args[2])*1000000;
		
		//add the list of URLs to the beginning of the HEADqueue
		for (String url : args[0].split(",")) {
			addToHeadQueue(new URL(url));
		}
			
		//set the FileWriter for logging for S3 
		directory = new File(args[3]);
		S3FileWriter.setDocFileWriter(directory);
		S3FileWriter.setUrlFileWriter(directory);
		//initialize URL to URL list map 	
		urlToUrlList = new HashMap<String, ArrayList<String>>();
		
		//Create thread pools to run the crawler
		Thread[] headPool = new Thread[50];
		Thread[] getPool = new Thread[50];
		for (int i = 0; i < 50; i++) {
			headPool[i] = new Thread(new HeadThreadRunnable());
			headPool[i].start();
			getPool[i] = new Thread(new GetThreadRunnable());
			getPool[i].start();
		}

		TimerTask s3WritingTask = new S3WritingTask();
		Timer s3Handler = new Timer(true);
		//wait 20 minutes to start, try every 20 minutes
		s3Handler.scheduleAtFixedRate(s3WritingTask, 1200000, 1200000);
		
		//add a shutdown hook to properly close DB
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				RefactoredWrapper.close();
				shutdown = true;
				System.out.println("Proper Shutdown.");
			}
		});
	}
}
