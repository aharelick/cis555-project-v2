package edu.upenn.cis555.crawler.storage;

import java.util.LinkedList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Site implements Comparable<Site> {

	@PrimaryKey
	private String url;
	@SecondaryKey(relate = Relationship.MANY_TO_ONE)
	private long nextRequestTime;
	private String contentType;
	private String body;
	private LinkedList<String> children;

	
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
	
	public long getNextRequestTime() {
		return nextRequestTime;
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

	@Override
	public int compareTo(Site arg0) {
		if (this.nextRequestTime < arg0.nextRequestTime) {
			return -1;
		} else if (this.nextRequestTime > arg0.nextRequestTime) {
			return 1;
		} else {
			return 0;
		}
	}

}
