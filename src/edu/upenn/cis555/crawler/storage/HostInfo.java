package edu.upenn.cis555.crawler.storage;

import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class HostInfo {
	
	@PrimaryKey
	private String hostname;
	
	private HashMap<String,ArrayList<String>> disallowedLinks;
	private HashMap<String,ArrayList<String>> allowedLinks;
	
	private HashMap<String,Integer> crawlDelays;
	private ArrayList<String> sitemapLinks;
	private ArrayList<String> userAgents;
	
	private long nextRequestTime;
	
	
	public HostInfo() {
		disallowedLinks = new HashMap<String,ArrayList<String>>();
		allowedLinks = new HashMap<String,ArrayList<String>>();
		crawlDelays = new HashMap<String,Integer>();
		sitemapLinks = new ArrayList<String>();
		userAgents = new ArrayList<String>();
	}
	
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	
	public String getHostname() {
		return hostname;
	}
	
	public void setNextRequestTime(long time) {
		nextRequestTime = time;
	}
	
	public void addDisallowedLink(String key, String value){
		if(!disallowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = disallowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
	}
	
	public void addAllowedLink(String key, String value){
		if(!allowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = allowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
	}
	
	public void addCrawlDelay(String key, Integer value){
		crawlDelays.put(key, value);
	}
	
	public void addSitemapLink(String val){
		sitemapLinks.add(val);
	}
	
	public void addUserAgent(String key){
		userAgents.add(key);
	}
	
	public boolean containsUserAgent(String key){
		return userAgents.contains(key);
	}
	
	public ArrayList<String> getDisallowedLinks(String key){
		return disallowedLinks.get(key);
	}
	
	public ArrayList<String> getAllowedLinks(String key){
		return allowedLinks.get(key);
	}
	
	public int getCrawlDelay(String key){
		return crawlDelays.get(key);
	}
	
	public void print(){
		for(String userAgent:userAgents){
			System.out.println("User-Agent: "+userAgent);
			ArrayList<String> dlinks = disallowedLinks.get(userAgent);
			if(dlinks != null)
				for(String dl:dlinks)
					System.out.println("Disallow: "+dl);
			ArrayList<String> alinks = allowedLinks.get(userAgent);
			if(alinks != null)
					for(String al:alinks)
						System.out.println("Allow: "+al);
			if(crawlDelays.containsKey(userAgent))
				System.out.println("Crawl-Delay: "+crawlDelays.get(userAgent));
			System.out.println();
		}
		if(sitemapLinks.size() > 0){
			System.out.println("# SiteMap Links");
			for(String sitemap:sitemapLinks)
				System.out.println(sitemap);
		}
	}
	
	public boolean crawlContainAgent(String key){
		return crawlDelays.containsKey(key);
	}
	
	/**
	 * Method to make http requests.
	 * @param url - the url to make the request to 
	 * @param reqType - HEAD or GET
	 * @param date - the date to be used in an if modified, null when not using that header
	 * @return
	 */
	public int crawlDelayFor555() {
		if (crawlDelays.containsKey("cis455crawler")) {
			return crawlDelays.get("cis455crawler");
		} else if (crawlDelays.containsKey("*")) {
			return crawlDelays.get("*");
		}
		// default crawl delay
		return 1;
	}
	
	/**
	 * Convenience method specifically for the user-agent cis455crawler.
	 * @param link
	 * @return
	 */
	public boolean disallowedLinkFor555(String link) {
		if (disallowedLinks.containsKey("cis455crawler")) {
			for (String s : disallowedLinks.get("cis455crawler")) {
				if (link.startsWith(s) || (link + "/").startsWith(s)) {
					return true;
				}
			}
			return false;
		} else if (disallowedLinks.containsKey("*")) {
			for (String s : disallowedLinks.get("*")) {
				if (link.startsWith(s) || (link + "/").startsWith(s)) {
					return true;
				}
			}
		}
		return false;
	}
}