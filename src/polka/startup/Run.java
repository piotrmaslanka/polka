package polka.startup;

import java.io.IOException;

import polka.netdispatch.DispatcherThread;
import polka.store.SeriesDB;
import polka.store.SeriesDefinitionDB;

public class Run {
	public static void main(String[] args) throws IOException, InterruptedException {	
		ConfigManager.loadConfig("config.json");
		SeriesDefinitionDB.getInstance();	// create the SeriesDefinitionDB
		SeriesDB.getInstance();				// create the SeriesDB

		DispatcherThread dt = new DispatcherThread();
		dt.start();
		if (ConfigManager.get().unix_socket_name != null) {
			DispatcherThread dtunix = new DispatcherThread(true);
			dtunix.start();
		}
		
		while (true) {
			Thread.sleep(10000);
		}
	}
}
