package edu.upenn.cis555.crawler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.upenn.cis555.crawler.storage.DBWrapper;
import edu.upenn.cis555.crawler.storage.S3FileWriter;
import edu.upenn.cis555.crawler.storage.Site;

public class Crawler {
	private static int portNumber = 5555;
	private static HashMap<Integer, URL> IPaddresses = new HashMap<Integer, URL>();
	private static long maxFileSize;
	private static File S3logDirectory;
	private static boolean shutdown = false;
	private static BQueue<Site> headQueue = new BQueue<Site>();
	private static BQueue<Site> getQueue = new BQueue<Site>();
	private static BQueue<Socket> requestQueue = new BQueue<Socket>();
	
	/**
	 * The main method initializes and runs the crawler. All threads used in
	 * the crawler and all databases are initialized here.
	 */
	public static void main(String[] args) {
		
		if (args.length != 5) {
			System.out.println("Usage: <comma separated start URLs>"
					+ " <path to BDB> <path to S3 log> <max size in MB> "
					+ "<comma separated list of ip addresses");
			System.exit(-1);
		}
		//initialize the static DBWrapper with the given path
		DBWrapper.init(args[1]);

		maxFileSize = Long.parseLong(args[3])*1000000;
		
		//populate the map of IP addresses
		String[] ips = args[4].split(",");
		for (int i = 0; i < ips.length; i++) {
			try {
				IPaddresses.put(i, new URL(ips[i]));
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
		//add the list of URLs to the beginning of the HeadQueue
		for (String url : args[0].split(",")) {
			DBWrapper.putToHeadQueue(new Site(url, System.currentTimeMillis()));
		}
			
		//set the directory for logging and initialize the FileWriter to S3
		S3logDirectory = new File(args[2]);
		S3FileWriter.setDocFileWriter(S3logDirectory);
		S3FileWriter.setUrlFileWriter(S3logDirectory);
		
		//Create a listener daemon that accepts Sockets on the given port
		Thread listener = new Thread(new ListenerRunnable());
		listener.setDaemon(true); //allows for graceful shutdown
		listener.start();	
		System.out.println("Listening on port " + portNumber);
		
		//Create pool of workers that handle requests
		Thread[] reqWorkerPool = new Thread[25];
		for (int i = 0; i < 25; i++) {	
			reqWorkerPool[i] = new Thread(new RequestWorkerRunnable());
			reqWorkerPool[i].start();	
		}
		
		//add a shutdown hook to properly close DB
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				DBWrapper.close();
				shutdown = true;
				System.out.println("Proper Shutdown.");
			}
		});
	}
	
	//=========================THREAD CLASSES==================================
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
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
	
	/**
	 * The Listener Thread is a daemon that listens for the administrator to
	 * start/stop/clear the queues or for other worker nodes sending a request
	 * to add a URL to the queue.
	 */
	static class ListenerRunnable implements Runnable {
		public void run() {
			try (
				ServerSocket serverSocket = new ServerSocket(portNumber, 1000);	
			) {
				while (true) {
					Socket clientSocket = serverSocket.accept();
					//take clientSocket and put it on the queue for worker threads
					if (shutdown) {
						System.out.println("Not accepting anymore requests");
					}
					requestQueue.enqueue(clientSocket);
				}		
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * The Request Worker Thread takes Sockets off of the requestQueue and 
	 * gets the stream, parses the request, and calls the appropriate 
	 * function based on the request parameters.
	 */
	static class RequestWorkerRunnable implements Runnable {	
    	public void run() {
    		while (!shutdown) {
        		Socket currentSocket = requestQueue.dequeue();
        		//This line is necessary for the shutdown call, see BQueue
        		if (currentSocket == null) {
        			break;
        		}
        		//helper below, calls start, stop, clear, or add to queue
        		parseRequest(currentSocket);
        		try {
					currentSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}
    }
	
	//======================HEAD and GET FUNCTIONS==============================
	static void processHead(Site url) {
		//need to check if the url's host already has a robots.txt in the DB
		
		//need to set the contentType of the URL!!!
	}
	
	static void processGet(Site url) {
		//Send a GET request to the given URL
		String body = null;
		String protocol = "";
		URL siteURL = null;
		try {
			siteURL = new URL(url.getSite());
			protocol = siteURL.getProtocol();
			if (protocol.equals("https")) {
				HttpsURLConnection res = https(new URL(url.getSite()), "GET");
				System.out.println("Downloading: " + url.getSite());
				body = inputStreamToString(res);
			} else if (protocol.equals("http")) {
				HttpURLConnection res = http(new URL(url.getSite()), "GET");
				System.out.println("Downloading: " + url.getSite());
				body = inputStreamToString(res);
			}
		} catch (MalformedURLException e) {
			return;
		}
		//Read the response and parse the document for links
		if (url.getContentType() == null) {
			return;
		} else if (url.getContentType().equals("text/html")) {
			ArrayList<LinkedList<String>> nodes =
					new ArrayList<LinkedList<String>>();
			LinkedList<String> children = new LinkedList<String>();
			Document doc = Jsoup.parse(body);
			for (Element e : doc.select("a[href]")) {
				URLWrapper child = new URLWrapper(e.attr("href"),
					protocol, siteURL.getHost(), siteURL.getPath());
				
				//if we extract a valid URL, hash and send along to correct node
				if (child.isGoodURL()) {
					children.add(child.getURL().toString());
					//get the correct node to send URL to
					int node = hashRange(child.getURL().getHost());
					nodes.get(node).add(child.getURL().toString());
				}		
			}
			//call client to send along lists of URLs for each node
			sendURLsToNodes(nodes);
			
			//write the body and list of URLs to the DB
			url.setChildren(children);
			url.setBody(body);
			//add this URL to the list of crawled URLs
			DBWrapper.putCrawledSite(url);
			
			return;
		} else {
			return;
		}
	}
	
	//=======================CONVENIENCE METHODS===============================
	/**
	 * Method to make http requests.
	 * @param url - the url to make the request to 
	 * @param reqType - HEAD or GET
	 */
	private static HttpURLConnection http(URL url, String reqType) {
		HttpURLConnection connect = null;
		try {
			connect = (HttpURLConnection) url.openConnection();
			connect.setRequestProperty("User-Agent", "cis455crawler");
			connect.setRequestMethod(reqType);
			connect.connect();
			return connect;
		} catch (IOException e) {
			e.printStackTrace();
			return null; 
		}
	}
	
	/**
	 * Method to make https requests.
	 * @param url - the url to make the request to 
	 * @param reqType - HEAD or GET
	 * @return
	 */
	private static HttpsURLConnection https(URL url, String reqType) {
		HttpsURLConnection connect = null;
		try {
			connect = (HttpsURLConnection) url.openConnection();
			connect.setRequestProperty("User-Agent", "cis455crawler");
			connect.setRequestMethod(reqType);
			connect.connect();
			return connect;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
		
	/**
	 * Convenience method to take a connection and return as String. 
	 */
	private static String inputStreamToString(URLConnection connect) {
		String body = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(connect.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		String line;
		try {
			while ((line = br.readLine()) != null) {
				body += line + "\n";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		body = body.trim();
		return body;
	}
	
	/**
	 * Method to hash the given host name and assign it a number of which
	 * node it should be sent to, based on the range of the hash.
	 */
	private static int hashRange(String host) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        digest.reset();
        digest.update(host.getBytes());
        BigInteger bigDigest = new BigInteger(1, digest.digest());
        BigInteger blank160 = BigInteger.ZERO.setBit(160);
		BigInteger max = blank160.subtract(BigInteger.ONE);
        BigInteger range = (max.add(BigInteger.ONE)).divide(
        		BigInteger.valueOf(IPaddresses.size()));
        int i;
        for (i = 1; i <= IPaddresses.size(); i++) {
        	if ((range.multiply(BigInteger.valueOf(i)).compareTo(bigDigest)) == 1) {
        		break;
        	}
        }
        return i - 1;
	}
	
	/**
	 * Method to read a list of all the URLs that should be sent to each node
	 * and then send a POST request with a body of the URLs.
	 */
	private static void sendURLsToNodes(ArrayList<LinkedList<String>> urls) {
		for (int i = 0; i < IPaddresses.size(); i++) {
			try {
				Socket socket = 
						new Socket(IPaddresses.get(i).getHost(), portNumber);
				OutputStream out = socket.getOutputStream();
				out.write(("POST " + "/urls" + " HTTP/1.0\r\n").getBytes());
				out.write("User-Agent: cis455crawler\r\n".getBytes());
				String output = "";
				for (String url : urls.get(i)) {
					output.concat(url + "\r\n");
				}
				out.write(("Content-Length: " + output.length() +
						"\r\n\r\n").getBytes());
				out.write(output.getBytes());
				out.write(("/r/n").getBytes());
				out.flush();
				out.close();
				socket.close();
			} catch(Exception e) {
				System.out.println(e);
			} 
		}	
	}
	
	//========================PRIVATE REQUEST HELPERS==========================
	/**
	 * This function is called to start the crawler threads after the main has
	 * already been started manually and it is listening on a server socket.
	 */
	private static void requestToStartCrawler() {
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
		//initialize the timer task for writing to S3	
		TimerTask s3WritingTask = new S3WritingTask();
		Timer s3Handler = new Timer(true);
		//wait 20 minutes to start, try every 20 minutes
		s3Handler.scheduleAtFixedRate(s3WritingTask, 1200000, 1200000);
	}
	
	/**
	 * This function is called to shutdown the crawler completely, needs a 
	 * manual restart.
	 */
	private static void requestToShutdownCrawler() {
		System.out.println("Shutting down the crawler.");
		shutdown = true;
		requestQueue.shutdown();
		headQueue.shutdown();
		getQueue.shutdown();
	}
	
	private static void requestToClearQueues() {
		System.out.println("Clearing queue of links from previous crawl");
	//TODO 
	}
	
	private static void requestToAddToHead(String url) {
		//need to make sure never in the crawled database
		 if (DBWrapper.getCrawledSite(url) == null) {
			//TODO add to the head with the appropriate crawl delay
		 }
			
		 
	}
	
	/**
	 * Parses the request picked off the queue by the request worker thread.
	 * Either starts, stops, clears the queue, or adds urls to the head
	 * @param socket
	 */
	private static void parseRequest(Socket socket) {
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			String line = in.readLine();
			String path = line.split(" ")[1];
			if (path.equals("start")) {
				requestToStartCrawler();
			} else if (path.equals("stop")) {
				requestToShutdownCrawler();
			} else if (path.equals("clear")) {
				requestToClearQueues();
			} else if (path.equals("urls")) {
				//parse through the head
				while(!line.equals("")) {
					line = in.readLine();
				}
				//should be at start of body
				while (line != null) {
					System.out.println(line);
					line = in.readLine();
					requestToAddToHead(line.trim());	
				}
			}		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
