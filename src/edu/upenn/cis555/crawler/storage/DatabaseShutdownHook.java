package edu.upenn.cis555.crawler.storage;


import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.SecondaryIndex;

/**
 * A shutdown hook that closes the environment and store anytime the program is shutdown
 */
public class DatabaseShutdownHook extends Thread{
	
	private Environment environment;
	private EntityStore hostStore;
	private EntityStore headStore;
	private EntityStore getStore;
	private EntityStore crawledStore;
	private EntityStore fileQueueStore;



	public DatabaseShutdownHook(Environment env, EntityStore store,
			EntityStore headStore, EntityStore getStore,
			EntityStore crawledStore, EntityStore fileQueueStore) {
		this.environment = env;
		this.hostStore = store;
		this.headStore = headStore;
		this.getStore = getStore;
		this.fileQueueStore = fileQueueStore;
		this.crawledStore = crawledStore;
	}
	
	public void run() {
		if (environment != null) {
			hostStore.close();
			headStore.close();
			getStore.close();
			crawledStore.close();
			fileQueueStore.close();
			environment.close();
			System.out.println("Closed the Database");
		}
	}
}
