package edu.upenn.cis555.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.upenn.cis555.crawler.storage.DBWrapper;
import edu.upenn.cis555.crawler.storage.HostInfo;
import edu.upenn.cis555.crawler.storage.S3FileWriter;
import edu.upenn.cis555.crawler.storage.Site;
import edu.upenn.cis555.crawler.storage.SiteBare;

public class Crawler {
	private static int portNumber = 5555;
	private static HashMap<Integer, URL> IPaddresses = new HashMap<Integer, URL>();
	private static long maxFileSize;
	private static File S3logDirectory;
	private static boolean shutdown = false;
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
			requestToAddToHead(url);
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
		Thread[] reqWorkerPool = new Thread[250];
		for (int i = 0; i < 250; i++) {	
			reqWorkerPool[i] = new Thread(new RequestWorkerRunnable());
			reqWorkerPool[i].start();	
		}
		requestToStartCrawler();
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
    			Site url = DBWrapper.conditionalPopHeadQueue();
        		if (url == null) {
        			try {
						Thread.sleep(2000);
        				System.out.println("HEAD: nothing ready to be taken");
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
        			continue;
        		}
				long delay;
				//Even though at top of priority queue, make sure not in future
				if ((delay = url.canCrawl()) < 0) {
					System.out.println("HEAD starting and not sleeping on: "
							+ url.getSite());
					processHead(url);
				} else {
					try {
						System.out.println("HEAD sleeping for " + (double) delay/1000 +
								" seconds on the site: " + url.getSite());
						Thread.sleep(delay);
						processHead(url);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}	
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
				Site url = DBWrapper.conditionalPopGetQueue();
        		if (url == null) {
        			try {
        				System.out.println("GET: nothing ready to be taken");
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
        			continue;
        		}
				long delay;
				//Even though at top of priority queue, make sure not in future
				if ((delay = url.canCrawl()) < 0) {
					System.out.println("GET starting and not sleeping on: "
							+ url.getSite());
					processGet(url);	
				} else {
					try {
						System.out.println("GET sleeping for " + (double) delay/1000 +
								" seconds on the site: " + url.getSite());
						Thread.sleep(delay);
						processGet(url);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
				Site writeMe = DBWrapper.popWriteToFileQueue();
				if (writeMe == null) {
					try {
						Thread.sleep(1500);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
					continue;
				}
				//then process the file by writing it to the S3 file
				String docLine = S3FileWriter.prepareFileLineDoc(
						writeMe.getSite(), writeMe.getBody());
				String urlLine = S3FileWriter.prepareFileLineUrlList(
						writeMe.getSite(), writeMe.getChildren());
				S3FileWriter.writeToDocFile(docLine);
				S3FileWriter.writeToUrlFile(urlLine);
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
				ServerSocket serverSocket = new ServerSocket(portNumber, 100000);	
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
	/**
	 * The main function for handling the first half of the crawl. Checks for 
	 * a robots.txt file, obeys it/fetches it, parses it, and/or sends a HEAD
	 * request to the site and adds to GET queue
	 */
	static void processHead(Site url) {	
		//need to check if the url's host already has a robots.txt in the DB
		URL siteURL = null;
		try {
			siteURL = new URL(url.getSite());
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}
		if (siteURL == null) { return; }
		String protocol = siteURL.getProtocol();
		HostInfo robots = DBWrapper.getHostInfo(siteURL.getHost());
		URLConnection connect = null;
		//the host has never been seen before, so need to fetch robots.txt
		if (robots == null) {
			URL robotsURL = null;
			try {
				robotsURL = new URL(protocol + "://" + 
							siteURL.getHost() + "/robots.txt");
			} catch (MalformedURLException e1) {
				e1.printStackTrace();
				return;
			}
			System.out.println("Fetching robots.txt for: " + siteURL.getHost());
			int responseCode = 0;
			if (protocol.equals("http")) {
				connect = http(robotsURL, "GET");
				if (connect == null) {
					return;
				}	
				try {
					responseCode = ((HttpURLConnection) connect).getResponseCode();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (protocol.equals("https")) {
				connect = https(robotsURL, "GET");
				if (connect == null) {
					return;
				}
				try {
					responseCode = ((HttpsURLConnection) connect).getResponseCode();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			String body;
			if (responseCode == 200) {
				body = inputStreamToString(connect);
				robots = parseRobots(body);
			} else {
				//TODO handle 301, 302, 304, 307
				robots = new HostInfo();
			}
			robots.setHostname(siteURL.getHost());
			long nextCrawlTime = (long) (System.currentTimeMillis() 
					+ robots.crawlDelayFor555()*1000);
			robots.setNextRequestTime(nextCrawlTime);
			//add the new robots.txt info to the database
			DBWrapper.putHostInfo(robots);
			//put the URL back on the head queue with updated next crawl time
			url.setNextRequestTime(nextCrawlTime);
	
			DBWrapper.putToHeadQueue(url);
			
			
		} else { //host already seen, proceed with head
			System.out.println("Robots already downloaded for " + url.getSite());
			if (robots.disallowedLinkFor555(url.getSite())) {
				System.out.println("Disallowed link: " + url.getSite());
				return;
			}
			//send the HEAD request
			if (protocol.equals("http")) {
				connect = http(siteURL, "HEAD");
			} else if (protocol.equals("https")){
				connect = https(siteURL, "HEAD");
			}
			if (connect == null) {
				return;
			}
			//handle the response code
			int responseCode = 0;
			String contentType;
			int contentLength;
			String redirect = null;
			
			if (protocol.equals("http")) {
				try {
					responseCode = ((HttpURLConnection) connect).getResponseCode();
				} catch (IOException e) {
					e.printStackTrace();
				}
				redirect = connect.getHeaderField("Location");
			} else if (protocol.equals("https")){
				try {
					responseCode = ((HttpsURLConnection) connect).getResponseCode();
				} catch (IOException e) {
					e.printStackTrace();
				}
				redirect = connect.getHeaderField("Location");
			}
			
			//interpret the code appropriately
			if (responseCode == 301 || responseCode == 307 ||
					responseCode == 302) {	
				//redirect and add to head queue if good url
				URLWrapper redir = new URLWrapper(redirect, null, null, null);
				if (redir.isGoodURL()) {
					System.out.println("Redirecting to: " + redirect);
					Site redSite = new Site(redir.getURL().toString(),
							robots.getNextRequestTime());
					DBWrapper.putToHeadQueue(redSite);
				}		
				return;	
			//200 OK, get content type and length and send to GET queue
			} else if (responseCode == 200) {
				contentType = connect.getContentType();
				url.setContentType(contentType);
				contentLength = connect.getContentLength();
				if (contentType == null) {
					return;
				} else if (!contentType.startsWith("text/html")) {
					return;
				}
				// content length
				if (contentLength > maxFileSize) {
					return;
				}
				//update crawl time and put to GET queue
				long nextCrawlTime = robots.getNextRequestTime();
				url.setNextRequestTime(nextCrawlTime);
		
				System.out.println("HEAD putting on GET: " + url.getSite());
				DBWrapper.putToGetQueue(url);
			}
		}
	}
	
	/**
	 * The main function for processing the second half of the crawl. Sends a 
	 * GET request to the given URL, parses the body if HTML, and extracts links
	 * that are then added back to the head queue.
	 */
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
				if (res == null) {
					return;
				}
				System.out.println("Downloading: " + url.getSite());
				body = inputStreamToString(res);
			} else if (protocol.equals("http")) {
				HttpURLConnection res = http(new URL(url.getSite()), "GET");
				if (res == null) {
					return;
				}
				System.out.println("Downloading: " + url.getSite());
				body = inputStreamToString(res);
			}
		} catch (MalformedURLException e) {
			return;
		}
		//Read the response and parse the document for links
		if (url.getContentType() == null) {
			return;
		} else if (url.getContentType().startsWith("text/html")) {
			LinkedList<String>[] nodes = new LinkedList[IPaddresses.size()];
			//initialize all of the linkedlists
			for (int i = 0; i < nodes.length; i++) {
				nodes[i] = new LinkedList<String>();
			}
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
					nodes[node].add(child.getURL().toString());
				}		
			}
			//call client to send along lists of URLs for each node
			sendURLsToNodes(nodes);
			
			//write the body and list of URLs to the DB
			Site crawled = new Site(url.getSite(), 0);
			crawled.setContentType("text/html");
			crawled.setBody(body);
			crawled.setChildren(children);
			DBWrapper.putWriteToFileQueue(crawled);	
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
	private static void sendURLsToNodes(LinkedList<String>[] urls) {
		for (int i = 0; i < IPaddresses.size(); i++) {
			try {
				Socket socket = 
						new Socket(IPaddresses.get(i).getHost(), portNumber);
				OutputStream out = socket.getOutputStream();
				out.write(("POST " + "/urls" + " HTTP/1.0\r\n").getBytes());
				out.write("User-Agent: cis455crawler\r\n".getBytes());
				String output = "";
				for (String url : urls[i]) {
			//		System.out.println("Node " + i + " is being sent " + url);
					output = output.concat(url + "\r\n");
				}
				out.write(("Content-Length: " + output.length() +
						"\r\n\r\n").getBytes());
				out.write(output.getBytes());
				out.flush();
				out.close();
				socket.close(); 
			} catch(Exception e) {
				System.out.println(e);
			} 
		}	
	}
	
	//============================PARSERS======================================
	/**
	 * Goes through the robots.txt string document and creates a HostInfo
	 * class for it. Puts everything in the appropriate hashmaps for easy access.
	 */
	private static HostInfo parseRobots(String robots) {
		HostInfo info = new HostInfo();
		String[] split = robots.split("\n\n");
		ArrayList<String> agents = new ArrayList<String>();
		ArrayList<String> disallow = new ArrayList<String>();
		ArrayList<String> allow = new ArrayList<String>();
		String delay = null;
		ArrayList<String> sitemap = new ArrayList<String>();
		for (int i = 0; i < split.length; i++) {
			String [] inner = split[i].split("\n|:");
			for (int j = 0; j < inner.length; j++) {
				if (j + 1 >= inner.length) {
					break;
				}
				if (inner[j].toLowerCase().trim().equals("user-agent")) {
					agents.add(inner[j + 1].trim());
				} else if (inner[j].toLowerCase().trim().equals("disallow")) {
					disallow.add(inner[j + 1].trim());
				} else if (inner[j].toLowerCase().trim().equals("allow")) {
					allow.add(inner[j + 1].trim());
				} else if (inner[j].toLowerCase().trim().equals("crawl-delay")) {
					delay = inner[j + 1].trim() ;
				} else if (inner[j].toLowerCase().trim().equals("sitemap")) {
					sitemap.add(inner[j + 1].trim());
				}
			}
			for (String agent : agents) {
				info.addUserAgent(agent);
				for (String link : disallow) {
					info.addDisallowedLink(agent, link);
				}
				for (String link : allow) {
					info.addAllowedLink(agent, link);
				}
				if (delay != null) {
					info.addCrawlDelay(agent, Double.parseDouble(delay));
				}
				for (String link : sitemap) {
					info.addSitemapLink(link);
				}
			}
			agents.clear();
			disallow.clear();
			allow.clear();
			sitemap.clear();
			delay = null;
		}
		return info;
	}
	
	/**
	 * Parses the request picked off the queue by the request worker thread.
	 * Either starts, stops, clears the queue, or adds urls to the head
	 * @param socket
	 */
	private static void parseRequest(Socket socket) {
		HashSet<String> duplicateURLs = new HashSet<String>();
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			String line = in.readLine();
			String path = line.split(" ")[1];
			if (path.equals("/start")) {
				requestToStartCrawler();
			} else if (path.equals("/stop")) {
				requestToShutdownCrawler();
			} else if (path.equals("/clear")) {
				requestToClearQueues();
			} else if (path.equals("/urls")) {
				//parse through the head
				while(!line.equals("")) {
					line = in.readLine();
				}
				//should be at start of body
				while ((line = in.readLine()) != null) {
					if (!duplicateURLs.contains(line.trim())) {
						requestToAddToHead(line.trim());
						duplicateURLs.add(line.trim());
					}
				}
			}		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//========================PRIVATE REQUEST HELPERS==========================
	/**
	 * This function is called to start the crawler threads after the main has
	 * already been started manually and it is listening on a server socket.
	 */
	private static void requestToStartCrawler() {
		//Create thread pools used in the crawler

		Thread[] headPool = new Thread[200];
		Thread[] getPool = new Thread[200];
		Thread[] fileWritingPool = new Thread[200];
		for (int i = 0; i < 200; i++) {

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
		//wait 15 seconds to start, try every 15 seconds
		s3Handler.scheduleAtFixedRate(s3WritingTask, 15000, 15000);
	}
	
	/**
	 * This function is called to shutdown the crawler completely, needs a 
	 * manual restart.
	 */
	private static void requestToShutdownCrawler() {
		System.out.println("Shutting down the crawler.");
		shutdown = true;
		requestQueue.shutdown();
	}
	
	/**
	 * This function is called to clear the queues but does not shutdown the 
	 * crawler.
	 */
	private static void requestToClearQueues() {
		System.out.println("Clearing queue of links from previous crawl");
		DBWrapper.clearGetQueue();
		DBWrapper.clearHeadQueue();
	}
	
	/**
	 * Takes in a String url, called from the parseRequest function, and 
	 * adds it to the headQueue if it has not yet been seen.
	 */
	private static void requestToAddToHead(String url) {
		//need to make sure never in the crawled database
		 if (DBWrapper.getCrawledSite(url) == null) {
			 DBWrapper.putCrawledSite(new SiteBare(url));
			//add to the head with the appropriate crawl delay
			 try {
				 long delay = DBWrapper.updateNextRequestTime(new URL(url).getHost());
				 System.out.println("Request worker is adding " + url);
				 System.out.println("with a delay of " + (delay - System.currentTimeMillis())/1000);
				 DBWrapper.putToHeadQueue(new Site(url,delay));
					
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		 }	else {
			 System.out.println("This site has previously been crawled: " + url);
		 }
	}	
}
