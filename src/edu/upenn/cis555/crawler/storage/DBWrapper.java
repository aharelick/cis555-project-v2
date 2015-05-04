package edu.upenn.cis555.crawler.storage;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.collections.StoredSortedMap;
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
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	
	private static Environment myEnv;
	private static EntityStore headStore;
	private static EntityStore getStore;
	private static EntityStore hostStore;
	private static EntityStore fileQueueStore;
	private static EntityStore crawledStore;
	
	
	private static PrimaryIndex<String, Site> headQueue;
	private static SecondaryIndex<Long, String, Site> headNextCrawlTime;
	private static PrimaryIndex<String, Site> getQueue;
	private static SecondaryIndex<Long, String, Site> getNextCrawlTime;
	private static PrimaryIndex<String, HostInfo> hostInfo;
	private static PrimaryIndex<String, SiteBare> crawledSites;
	private static PrimaryIndex<String, Site> writeToFileQueue;
	
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
        headStore = new EntityStore(myEnv, "headStore", storeConfig);
        headQueue = headStore.getPrimaryIndex(String.class, Site.class);
        headNextCrawlTime = headStore.getSecondaryIndex(headQueue, Long.class, "nextRequestTime");
        getStore = new EntityStore(myEnv, "getStore", storeConfig);
        getQueue = getStore.getPrimaryIndex(String.class, Site.class);
        getNextCrawlTime = getStore.getSecondaryIndex(getQueue, Long.class, "nextRequestTime");
        hostStore = new EntityStore(myEnv, "hostStore", storeConfig);
        hostInfo = hostStore.getPrimaryIndex(String.class, HostInfo.class);
        crawledStore = new EntityStore(myEnv, "crawledStore", storeConfig);
        crawledSites = crawledStore.getPrimaryIndex(String.class, SiteBare.class);
        fileQueueStore = new EntityStore(myEnv, "fileQueueStore", storeConfig);
        writeToFileQueue = fileQueueStore.getPrimaryIndex(String.class, Site.class);
        DatabaseShutdownHook hook = new DatabaseShutdownHook(myEnv, hostStore,
        		headStore, getStore, crawledStore, fileQueueStore);
        Runtime.getRuntime().addShutdownHook(hook);
        System.out.println("Database Started");
	}
	
	/**
	 * Acquires a write lock then does gets a cursor and iterates
	 * over the index until it gets and deletes as many entities as requested.
	 * @param count
	 */
	public synchronized static LinkedList<Site> batchPullFromHead(int count) {
		EntityCursor<Site> cursor = headQueue.entities();
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
			sync();
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
	public synchronized static LinkedList<Site> batchPullFromGet(int count) {
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
		return hostInfo.get(host);
	}
	
	public synchronized static long updateNextRequestTime(String host) {
		Database db = hostInfo.getDatabase();
    	EntityBinding<HostInfo> binding = hostInfo.getEntityBinding();
    	EntryBinding<String> keybinding = hostInfo.getKeyBinding();
    	DatabaseEntry key = new DatabaseEntry(); 
    	keybinding.objectToEntry(host, key);
		Cursor cursor = db.openCursor(null, null);
	    DatabaseEntry data = new DatabaseEntry();
	    if (cursor.getSearchKey(key, data, LockMode.RMW) == OperationStatus.SUCCESS) {
	    	HostInfo info = binding.entryToObject(key, data);
	    	info.setNextRequestTime((long) (info.getNextRequestTime() +
	    			(info.crawlDelayFor555() * 1000)));
	    	binding.objectToData(info, data);
	    	cursor.putCurrent(data);
	    	cursor.close();
	    	return info.getNextRequestTime();
	    } else {
	    	cursor.close();
	    	return 0;
	    }
	}
	
	public static void putHostInfo(HostInfo info) {
		hostInfo.put(info);
	}
	
	public static void deleteHostInfo(String host) {
		hostInfo.delete(host);
	}
	
	public static SiteBare getCrawledSite(String key) {
		return crawledSites.get(key);
	}
	
	public static void putCrawledSite(SiteBare site) {
		crawledSites.put(site);
	}

	public static void deleteCrawledSite(String key) {
		crawledSites.delete(key);
	}
	
	public static Site popWriteToFileQueue() {
		Site site;
		synchronized (writeToFileQueue) {
			EntityCursor<Site> cursor = writeToFileQueue.entities();
			site = cursor.next(LockMode.RMW);
			if (site != null) {
				cursor.delete();
			}	
			cursor.close();
		}
		return site;
	}
	
	public static void putWriteToFileQueue(Site site) {
		synchronized (writeToFileQueue) {
			writeToFileQueue.put(site);
		}
	}
	
	public static void clearGetQueue() {
		EntityCursor<Site> cursor = getQueue.entities();
		while (cursor.next() != null) {
			cursor.delete();
		}
	}
	
	public static void clearHeadQueue() {
		EntityCursor<Site> cursor = headQueue.entities();
		while (cursor.next() != null) {
			cursor.delete();
		}
	}
	
	public synchronized static Site popHeadQueue() {
		Site site;
		EntityCursor<Site> cursor = headNextCrawlTime.entities();
		site = cursor.first(LockMode.RMW);
		if (site != null) {
			cursor.delete();
		}
		cursor.close();
		return site; 
	}
	
	public synchronized static Site popGetQueue() {
		Site site;
		EntityCursor<Site> cursor = getNextCrawlTime.entities();
		site = cursor.first(LockMode.RMW);
		if (site != null) {
			cursor.delete();
		}
		cursor.close();
		return site;
	}
	
	public synchronized static Site conditionalPopHeadQueue() {
		Site site;
		EntityCursor<Site> cursor = headNextCrawlTime.entities();
		site = cursor.first(LockMode.RMW);
		if (site != null && site.getNextRequestTime() - System.currentTimeMillis() < 2000) {
			cursor.delete();
			cursor.close();
			return site;
		} else {
			cursor.close();
			return null;
		}
	}
	
	public synchronized static Site conditionalPopGetQueue() {
		Site site;
		EntityCursor<Site> cursor = getNextCrawlTime.entities();
		site = cursor.first(LockMode.RMW);
		if (site != null && site.getNextRequestTime() - System.currentTimeMillis() < 2000) {
			cursor.delete();
			cursor.close();
			return site;
		} else {
			cursor.close();
			return null;
		}
	}

	public static void sync() {
	//	store.sync();
	}
	
	public static void close() {
		hostStore.close();
		headStore.close();
		getStore.close();
		crawledStore.close();
		fileQueueStore.close();
		myEnv.close();
	}
}
	
	
