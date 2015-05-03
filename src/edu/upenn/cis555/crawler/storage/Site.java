package edu.upenn.cis555.crawler.storage;

import java.util.LinkedList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Site {

	@PrimaryKey
	private String url;
	private String contentType;
	private String body;
	private LinkedList<String> children;
	private long nextRequestTime;

	
	public Site() {
		
	}
	
	public Site(String url, long nextRequestTime) {
		this.url = url;
		this.nextRequestTime = nextRequestTime;
		children = new LinkedList<String>();
	}
	
	public String getSite() {
		return url;
	}
	
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	
	public void setNextRequestTime(long nextRequestTime) {
		this.nextRequestTime = nextRequestTime;
	}
	
	public String getContentType() {
		return contentType;
	}
	
	public void setBody(String body) {
		this.body = body;
	}
	
	public void setChildren(LinkedList<String> children) {
		this.children = children;
	}
	
	public String getBody() {
		return body;
	}
	
	public LinkedList<String> getChildren() {
		return children;
	}
	
	
	
	/**
	 * If return value is negative, the time to crawl has already
	 * passed and you can send another request.
	 * @return
	 */
	public long canCrawl() {
		return nextRequestTime - System.currentTimeMillis();
	}

}
