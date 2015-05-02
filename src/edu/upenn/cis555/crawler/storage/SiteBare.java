package edu.upenn.cis555.crawler.storage;

import java.net.URL;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class SiteBare {
	
	@PrimaryKey
	private String url;
	
	public SiteBare() {
		
	}
	
	public SiteBare(String url) {
		this.url = url;
	}
}
