package edu.upenn.cis555.crawler;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class URLWrapper {
	URL url = null;
	boolean goodURL = true;

	/**
	 * Wrapper method for the URL. Parses the given string 
	 * and makes it a full URL with protocol and hostname.
	 * @param link
	 * @param protocol
	 * @param hostname
	 * @param path
	 */
	public URLWrapper(String link, String protocol, String hostname, String path) {
		try {
			URI uri = new URI(link);
			
			if (link.startsWith("//")) {
				link = protocol + ":" + link; 
				url = new URL(link);
				return;
			}
			if ((path != null) && (path.trim().equals(""))) {
				path = "/";
			}
			if ((path != null) && (path.substring(path.lastIndexOf('/'))).contains(".")) {
				path = path.substring(0, path.lastIndexOf('/')).trim();
			}
			if ((path != null) && !path.endsWith("/")) {
				path += "/";
			}
			if (!uri.isAbsolute() && (uri.getHost() == null) && (hostname != null)) {
				if (!link.startsWith("/")) {
					link = hostname + path  + link;
				} else {
					link = hostname + link;
				}
			}

			if (!uri.isAbsolute() && (uri.getScheme() == null) && (protocol != null)) {
				link = protocol + "://" + link;
			}
			url = new URL(link);
		} catch (MalformedURLException | URISyntaxException e) {
			goodURL = false;
		}
	}
	
	public URL getURL() {
		return url;
	}
	
	public boolean isGoodURL() {
		return goodURL;
	}
}