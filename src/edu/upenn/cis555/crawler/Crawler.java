package edu.upenn.cis555.crawler;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Timer;
import java.util.TimerTask;

import edu.upenn.cis555.crawler.storage.DBWrapper;
import edu.upenn.cis555.crawler.storage.S3FileWriter;
import edu.upenn.cis555.crawler.storage.Site;

public class Crawler {
	private static long maxFileSize;
	private static File S3logDirectory;
	private static boolean shutdown = false;
	private static BQueue<Site> headQueue = new BQueue<Site>();
	private static BQueue<Site> getQueue = new BQueue<Site>();
	
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

		maxFileSize = Long.parseLong(args[3])*1000000;
		
		//add the list of URLs to the beginning of the HeadQueue
		for (String url : args[0].split(",")) {
			DBWrapper.putToHeadQueue(new Site(url, System.currentTimeMillis()));
		}
			
		//set the directory for logging and initialize the FileWriter to S3
		S3logDirectory = new File(args[2]);
		S3FileWriter.setDocFileWriter(S3logDirectory);
		S3FileWriter.setUrlFileWriter(S3logDirectory);
		
		//Create thread pools used in the crawler
		Thread[] headPool = new Thread[50];
		Thread[] getPool = new Thread[50];
		Thread[] fileWritingPool = new Thread[50];
		for (int i = 0; i < 50; i++) {
			headPool[i] = new Thread(new HeadThreadRunnable());
			headPool[i].start();
			getPool[i] = new Thread(new GetThreadRunnable());
			getPool[i].start();
			fileWritingPool[i] = new Thread(new FileWriterThreadRunnable());
			fileWritingPool[i].start();	
		}

		TimerTask s3WritingTask = new S3WritingTask();
		Timer s3Handler = new Timer(true);
		//wait 20 minutes to start, try every 20 minutes
		s3Handler.scheduleAtFixedRate(s3WritingTask, 1200000, 1200000);
		
		//add a shutdown hook to properly close DB
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				DBWrapper.close();
				shutdown = true;
				System.out.println("Proper Shutdown.");
			}
		});
	}
	
	/**
	 * The HeadThread class takes URLs from the HeadQueue, check for the
	 * robots.txt file if it hasn't been fetched already, and then sends 
	 * a HEAD request to determine if it should download the page.
	 */
	static class HeadThreadRunnable implements Runnable {	
    	public void run() {
    		while (!shutdown) { 
				Site url = headQueue.dequeue();
				//This line is necessary for the shutdown call, see BQueue.java
        		if (url == null) {
        			break;
        		}
				long delay;
				//Even though at top of priority queue, make sure not in future
				if ((delay = url.canCrawl()) < 0) {
					processHead(url);
				} else {
					Thread.sleep(delay);
					processHead(url);
				}
    		}
    	}
    }
	
	/**
	 * The GETThread class takes URLs from the GETqueue, downloads the page,
	 * if the page is HTML it extracts links, otherwise it checks XPaths and 
	 * updates channels if necessary.
	 */
	static class GetThreadRunnable implements Runnable {	
		public void run() {
			while (!shutdown) { 
				Site url = getQueue.dequeue();
				//This line is necessary for the shutdown call, see BQueue.java
        		if (url == null) {
        			break;
        		}
				long delay;
				//Even though at top of priority queue, make sure not in future
				if ((delay = url.canCrawl()) < 0) {
					processGet(url);
				} else {
					Thread.sleep(delay);
					processGet(url);
				}
    		}
		}
	}
	
	/**
	 * The FileWriter Thread class handles writing the documents and the 
	 * lists of URLs to the files in EBS that are sent to S3 periodically
	 */
	static class FileWriterThreadRunnable implements Runnable {	
		public void run() {
			while (!shutdown) { 
			//need to pick off file queue from DB (no need to buffer)
				//then process the file by writing it to the S3 file
    		}
		}
	}
	
	/**
	 * Timer task that switches the current file being wrote to and sends the 
	 * previous files to S3 at the given period defined in the main method.
	 */
	static class S3WritingTask extends TimerTask {		
		public void run() {
			S3FileWriter.switchFileAndWriteToS3(S3logDirectory);		
		}
	}
}
