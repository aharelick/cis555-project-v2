package edu.upenn.cis555.legacy;


import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;

import org.apache.stanbol.enhancer.engines.htmlextractor.impl.DOMBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;

import com.sleepycat.persist.EntityCursor;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLWrapper;
import edu.upenn.cis455.storage.Channel;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.Site;


public class AlexXPathCrawler {
	private static int numberOfFiles;
	private static int maxFileSize; // in bytes
	private static LinkedList<URLWrapper> queue = new LinkedList<URLWrapper>();
	private static int crawledCount = 1;
	private static Set<String> currentCrawl = new HashSet<String>();
	
	
	/**
	 * Parses the arguments and starts crawling.
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 3 && args.length != 4) {
			System.out.println("Usage: <start url> <BerkeleyDB dir> <max doc size> <# of files (opt.)>");
			System.exit(0);
		}
		numberOfFiles = Integer.MAX_VALUE;
		if (args.length == 4) {
			numberOfFiles = Integer.parseInt(args[3]);
		}
		String startURL = args[0];
		String berkeleyDir = args[1];
		maxFileSize = Integer.parseInt(args[2]) * 1000000;
		DBWrapper.createDB(berkeleyDir);
		URLWrapper curr = new URLWrapper(startURL, null, null, null);
		queue.add(curr);
		crawl();
		DBWrapper.close();
	}
	
	/**
	 * Pops urls off the queue. 
	 * Sees if the urls are valid.
	 * Sees if we have already crawled the link.
	 * Sees if we have hit the limit and either continues or breaks.
	 */
	private static void crawl() {
		while (!queue.isEmpty()) {
			URLWrapper curr = queue.remove();
			if (!curr.isGoodURL()) {
				continue;
			}
			if (currentCrawl.contains(curr.getURL().toString())) {
				continue;
			}
			System.out.println("--------------- crawling page " + (crawledCount) + "-----------------------------------");
			currentCrawl.add(curr.getURL().toString());
			request(curr);
			if ((crawledCount) == numberOfFiles) {
				break;
			}
			if (!queue.isEmpty()) {
				crawledCount++;
			}
		}
		System.out.println("--------------------------------------------------");
		System.out.println("you crawled " + crawledCount + " pages");
	}
	
	/**
	 * Is the content type html.
	 * @param input
	 * @return
	 */
	private static boolean isHTML(String input) {
		return input.startsWith("text/html");
	}
	
	/**
	 * Is it considered an xml document.
	 * @param input
	 * @return
	 */
	private static boolean isXML(String input) {
		return input.startsWith("text/xml") || 
				input.startsWith("application/xml") ||
				input.endsWith("+xml");
	}
	
	/**
	 * Creates the link to the <hostname>/robots.txt from the 
	 * url given.
	 * @param url
	 * @return
	 */
	private static URL constructRobots(URL url) {
		String robots = url.getProtocol();
		robots += "://";
		robots += url.getHost();
		robots += "/robots.txt";
		try {
			return (new URL(robots));
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Convenience method to take a connection and return the 
	 * string contained inside it. 
	 * @param is
	 * @param connect
	 * @return
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
	 * Goes through the robots.txt string document and creates a RobotsTxtInfo
	 * class for it. Puts everything in the appropriate hashmaps for easy access.
	 * @param robots
	 * @return
	 */
	private static RobotsTxtInfo parseRobots(String robots) {
		RobotsTxtInfo info = new RobotsTxtInfo();
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
					info.addCrawlDelay(agent, Integer.parseInt(delay));
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
	 * Convenience method to sleep to avoid writing a ton of try-catches
	 * @param sleep
	 */
	private static void sleep(int sleep) {
		try {
			Thread.sleep(sleep * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * The meat of the class. Takes in the URLWrapper and decides whether it's http or https.
	 * Sends the GET request to robots.
	 * Sends the HEAD request to the URL.
	 * Sends the GET request to the URL.
	 * Looks at the filetype and evaluates if xml and parses for a[href] if html.
	 * @param wrapper
	 */
	private static void request(URLWrapper wrapper) {
		URL robotsURL = constructRobots(wrapper.getURL());
		if (robotsURL == null) {
			return;
		}
		boolean http = (robotsURL.getProtocol().equals("http"));
		System.out.println("getting robots.txt for: " + robotsURL.toString());
		int robotsResponseCode = 0;
		URLConnection connect = null;
		if (http) {
			connect = http(robotsURL, "GET", null);
			try {
				robotsResponseCode = ((HttpURLConnection) connect).getResponseCode();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			connect = https(robotsURL, "GET", null);
			try {
				robotsResponseCode = ((HttpsURLConnection) connect).getResponseCode();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		boolean robotsFromDB = false;
		System.out.println("robots.txt status code: " + robotsResponseCode);
		RobotsTxtInfo robots;
		String body;
		if (((robots = DBWrapper.getRobots(robotsURL.getHost())) == null) &&
			(robotsResponseCode == 200)) {
			body = inputStreamToString(connect);
			System.out.println("got body of robots.txt");
			robots = parseRobots(body);
			robots.setHostname(robotsURL.getHost());
			DBWrapper.putRobots(robots);
			System.out.println("inserted robots into db");
		} else if (robots == null) {
			System.out.println("making new robots.txt");
			robots = new RobotsTxtInfo();
		} else {
			robotsFromDB = true;
			System.out.println("got robots.txt from db");
		}
		if (robots.disallowedLinkFor455(wrapper.getURL().getPath())) {
			System.out.println("link is disallowed by robots: " + wrapper.getURL().getPath());
			return;
		}
		if (!robotsFromDB) {
			System.out.println("sleeping for " + robots.crawlDelayFor455() + " seconds");
			sleep(robots.crawlDelayFor455());
		}
		// HEAD request to the site
		Site site;
		if ((site = DBWrapper.getSite(wrapper.getURL().toString())) == null) {
			System.out.println(wrapper.getURL().toString() + " isn't in db");
			if (http) {
				connect = http(wrapper.getURL(), "HEAD", null);
			} else {
				connect = https(wrapper.getURL(), "HEAD", null);
			}
		} else {
			System.out.println(wrapper.getURL().toString() + " is in db at " + site.getDownloadedAt() + ", fetching");
			if (http) {
				connect = http(wrapper.getURL(), "HEAD", site.getDownloadedAt());
			} else {
				connect = https(wrapper.getURL(), "HEAD", site.getDownloadedAt());
			}
			site.setTime();
		}
		int headResponseCode = 0;
		String contentType;
		int contentLength;
		String redirect = null;
		if (http) {
			try {
				headResponseCode = ((HttpURLConnection) connect).getResponseCode();
			} catch (IOException e) {
				e.printStackTrace();
			}
			redirect = connect.getHeaderField("Location");
		} else {
			try {
				headResponseCode = ((HttpsURLConnection) connect).getResponseCode();
			} catch (IOException e) {
				e.printStackTrace();
			}
			redirect = connect.getHeaderField("Location");
		}
		System.out.println("head status code: " + headResponseCode);
		// response codes
		if (headResponseCode == 301 || headResponseCode == 307 ||
				headResponseCode == 302) {
			System.out.println("adding " + redirect + " to queue and returning");
			queue.add(new URLWrapper(redirect, null, null, null));
			return;	
		} else if (headResponseCode == 200) {
				contentType = connect.getContentType();
				contentLength = connect.getContentLength();
			if (contentType == null) {
				return;
			}
			// content length
			if (contentLength > maxFileSize) {
				return;
			}
			System.out.println("sleeping for " + robots.crawlDelayFor455() + " seconds");
			sleep(robots.crawlDelayFor455());
			System.out.println("sending get request to " + wrapper.getURL().toString());
			if (http) {
				connect = http(wrapper.getURL(), "GET", null);
				body = inputStreamToString(connect);
			} else {
				connect = https(wrapper.getURL(), "GET", null);
				body = inputStreamToString(connect);
			}
			System.out.println("got body of site from get req");
			System.out.println("adding site to db: " + wrapper.getURL().toString());
			site = new Site(wrapper.getURL().toString(), body, contentType);
			DBWrapper.putSite(site);
		} else if (headResponseCode == 304) {
			contentType = site.getContentType();
			site.setTime();
			body = site.getBody();
			System.out.println("got body of site from db");
		} else {
			return;
		}
			// content type
		if (isXML(contentType)) {
			Document jsoupDoc = Jsoup.parse(body, "", Parser.xmlParser());
			org.w3c.dom.Document w3Doc = DOMBuilder.jsoup2DOM(jsoupDoc);
			EntityCursor<Channel> cursor = DBWrapper.getChannelCursor();
			Channel entity;
			
		    while ((entity = cursor.next()) != null) {
		    	entity.removeSite(site.getSite());
		    	if (entity.evaluateDoc(w3Doc)) {
		    		System.out.println("adding to channel: " + entity.getName());
		    		entity.addSite(site.getSite());
		    		DBWrapper.putChannel(entity);
		    	}
		    }
		    cursor.close();
		} else if (isHTML(contentType)) {
			System.out.println("got an html doc");
			Document doc = Jsoup.parse(body);
			System.out.println("there are " + doc.select("a[href]").size() + " children");
			for (Element e : doc.select("a[href]")) {
				URLWrapper child = new URLWrapper(e.attr("href"),
					wrapper.getURL().getProtocol(), wrapper.getURL().getHost(), wrapper.getURL().getPath());
				queue.add(child);
			}
			return;
		} else {
			return;
		}		
	}
	
	/**
	 * Method to make http requests.
	 * @param url - the url to make the request to 
	 * @param reqType - HEAD or GET
	 * @param date - the date to be used in an if modified, null when not using that header
	 * @return
	 */
	private static HttpURLConnection http(URL url, String reqType, String date) {
		HttpURLConnection connect = null;
		try {
			connect = (HttpURLConnection) url.openConnection();
			connect.setRequestProperty("User-Agent", "cis455crawler");
			if (date != null) {
				connect.setRequestProperty("If-Modified-Since", date);
			}
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
	 * @param date - the date to be used in an if modified, null when not using that header
	 * @return
	 */
	private static HttpsURLConnection https(URL url, String reqType, String date) {
		HttpsURLConnection connect = null;
		try {
			connect = (HttpsURLConnection) url.openConnection();
			connect.setRequestProperty("User-Agent", "cis455crawler");
			if (date != null) {
				connect.setRequestProperty("If-Modified-Since", date);
			}
			connect.setRequestMethod(reqType);
			connect.connect();
			return connect;
		} catch (IOException e) {
			e.printStackTrace();
			return null; 
		}
	}
}
