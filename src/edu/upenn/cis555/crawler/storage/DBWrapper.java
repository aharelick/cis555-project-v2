package edu.upenn.cis555.crawler.storage;

import java.io.File;
import java.util.LinkedList;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	
	private static Environment myEnv;
	private static EntityStore store;
	
	private static PrimaryIndex<String, Site> headQueue;
	private static PrimaryIndex<String, Site> getQueue;
	private static PrimaryIndex<String, HostInfo> hostInfo;
	private static PrimaryIndex<String, Site> crawledSites;
	
	/**
	 * Create the DB if it doesn't exist and open it if it does exist.
	 * @param dbdir - the path to the database location
	 */
	
	public static void init(String dbdir) {

		File dir = new File(dbdir);
		boolean success = dir.mkdirs();
		if (success) {
			System.out.println("Created the database");
		}
		// Open the environment, creating one if it does not exist
		// Open the store, creating one if it does not exist
        EnvironmentConfig envConfig = new EnvironmentConfig();
        StoreConfig storeConfig = new StoreConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        
        myEnv = new Environment(dir,envConfig);
        System.out.println(dir.getAbsolutePath());
        store = new EntityStore(myEnv, "EntityStore", storeConfig);
        headQueue = store.getPrimaryIndex(String.class, Site.class);
        getQueue = store.getPrimaryIndex(String.class, Site.class);
        hostInfo = store.getPrimaryIndex(String.class, HostInfo.class);
        crawledSites = store.getPrimaryIndex(String.class, Site.class);
        DatabaseShutdownHook hook = new DatabaseShutdownHook(myEnv, store);
        Runtime.getRuntime().addShutdownHook(hook);
        System.out.println("Database Started");
	}
	
	/**
	 * Acquires a write lock then does gets a cursor and iterates
	 * over the index until it gets and deletes as many entities as requested.
	 * @param count
	 */
	public static LinkedList<Site> batchPullFromHead(int count) {
		EntityCursor<Site> cursor = headQueue.entities();
		LinkedList<Site> sites = new LinkedList<Site>();
		try {
			Site entity;
			while ((entity = cursor.next(LockMode.RMW)) != null) {
				sites.add(entity);
				System.out.println(entity.getSite());
				cursor.delete();
				if (sites.size() == count) {
					break;
				}
			}
		} finally {
			cursor.close();
		}	
		return sites;
	}
	
	public static void putToHeadQueue(Site site) {
		headQueue.put(site);
	}
	
	public static void deleteFromHeadQueue(String url) {
		headQueue.delete(url);
	}
	
	/**
	 * Acquires a write lock then does gets a cursor and iterates
	 * over the index until it gets and deletes as many entities as requested.
	 * @param count
	 */
	public static LinkedList<Site> batchPullFromGet(int count) {
		EntityCursor<Site> cursor = getQueue.entities();
		LinkedList<Site> sites = new LinkedList<Site>();
		try {
			Site entity;
			while ((entity = cursor.next(LockMode.RMW)) != null) {
				sites.add(entity);
				cursor.delete();
				if (sites.size() == count) {
					break;
				}
			}
		} finally {
			cursor.close();
		}	
		return sites;
	}
	
	public static void putToGetQueue(Site site) {
		getQueue.put(site);
	}
	
	public static void deleteFromGetQueue(String url) {
		getQueue.delete(url);
	}
	
	public static HostInfo getHostInfo(String host) {
		Database db = hostInfo.getDatabase();
    	EntityBinding<HostInfo> binding = hostInfo.getEntityBinding();
		HostInfo tempHost = new HostInfo();
		tempHost.setHostname(host);
		DatabaseEntry key = new DatabaseEntry();
		binding.objectToKey(tempHost, key);
		Cursor cursor = db.openCursor(null, null);
	    DatabaseEntry data = new DatabaseEntry();
	    if (cursor.getSearchKey(key, data, LockMode.RMW) == OperationStatus.SUCCESS) {
	    	HostInfo info = binding.entryToObject(key, data);
	    	cursor.close();
	    	return info;
	    } else {
	    	cursor.close();
	    	return null;
	    }
	}
	
	public static void putHostInfo(HostInfo info) {
		hostInfo.put(info);
	}
	
	public static void deleteHostInfo(String host) {
		hostInfo.delete(host);
	}
	
	public static Site getCrawledSite(String key) {
		return crawledSites.get(key);
	}
	
	public static void putCrawledSite(Site site) {
		crawledSites.put(site);
	}

	public static void deleteCrawledSite(String key) {
		crawledSites.delete(key);
	}

	public static void sync() {
		//store.sync();
	}
	
	public static void close() {
		store.close();
		myEnv.close();
	}
}
	
	
