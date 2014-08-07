package polka.startup;

import java.io.IOException;

import polka.gcollector.GarbageCollectionThread;
import polka.gossip.GossipOutbound;
import polka.gossip.GossipThread;
import polka.gossip.NodeDB;
import polka.gossip.messages.GossipAdvertise;
import polka.netdispatch.DispatcherThread;
import polka.repair.ReparatorySupervisorThread;
import polka.store.SeriesDB;
import polka.store.SeriesDefinitionDB;

public class Run {

	
	public static void main(String[] args) throws IOException, InterruptedException {	
		ConfigManager.loadConfig("config.json");
		NodeDB.getInstance();				// create the NodeDB
		SeriesDefinitionDB.getInstance();	// create the SeriesDefinitionDB
		SeriesDB.getInstance();				// create the SeriesDB

		ReparatorySupervisorThread.getInstance().start();
		GossipThread.getInstance().start();
		new GarbageCollectionThread().start();
		DispatcherThread dt = new DispatcherThread();
		dt.start();
		if (ConfigManager.get().unix_socket_name != null) {
			DispatcherThread dtunix = new DispatcherThread(true);
			dtunix.start();
		}
		
		// Should we bootstrap?
		if (ConfigManager.get().bootstrap != null)
			new GossipOutbound(
					GossipAdvertise.from_nodeinfo(NodeDB.getInstance().dump(), true), 
					ConfigManager.get().bootstrap
				).executeAsThread();
		
		
		while (true) {
			Thread.sleep(10000);
		}
	}
}
