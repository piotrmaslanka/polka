package zero.startup;

import java.io.IOException;
import java.nio.ByteBuffer;

import zero.H;
import zero.gossip.GossipOutbound;
import zero.gossip.GossipThread;
import zero.gossip.NodeDB;
import zero.gossip.messages.GossipAdvertise;
import zero.lfds.LFDAlreadyExistsException;
import zero.lfds.LFDDamagedException;
import zero.netdispatch.DispatcherThread;
import zero.repair.ReparatorySupervisorThread;
import zero.store.SeriesDB;
import zero.store.SeriesDefinitionDB;

@SuppressWarnings("unused")
public class Run {

	
	public static void main(String[] args) throws IOException, InterruptedException, LFDDamagedException, LFDAlreadyExistsException, IllegalArgumentException {		
		ConfigManager.loadConfig("config.json");
		NodeDB.getInstance();				// create the NodeDB
		SeriesDefinitionDB.getInstance();	// create the SeriesDefinitionDB
		SeriesDB.getInstance();				// create the SeriesDB

		ReparatorySupervisorThread.getInstance().start();
		GossipThread.getInstance().start();
		DispatcherThread dt = new DispatcherThread();
		dt.start();
		
		// Should we bootstrap?
		if (ConfigManager.get().bootstrap != null)
			new GossipOutbound(
					GossipAdvertise.from_nodeinfo(NodeDB.getInstance().dump(), true), 
					ConfigManager.get().bootstrap
				).executeAsThread();
		
		
		while (true) Thread.sleep(10000);
	}
}
