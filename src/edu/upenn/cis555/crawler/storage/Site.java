package edu.upenn.cis555.crawler.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Site {

	@PrimaryKey
	private String url;
	private String contentType;
	private long nextRequestTime;

	
	public Site() {
		
	}
	
	public Site(String url, String contentType, long nextRequestTime) {
		this.url = url;
		this.contentType = contentType;
		this.nextRequestTime = nextRequestTime;
	}
	
	public String getSite() {
		return url;
	}
	
	public String getContentType() {
		return contentType;
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
