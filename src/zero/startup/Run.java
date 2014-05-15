package zero.startup;

import java.io.IOException;

import zero.H;
import zero.gossip.GossipOutbound;
import zero.gossip.GossipThread;
import zero.gossip.NodeDB;
import zero.gossip.messages.GossipAdvertise;
import zero.ifc.DefinitionMismatchException;
import zero.ifc.LinkBrokenException;
import zero.ifc.LocalInterface;
import zero.ifc.NetworkInterface;
import zero.ifc.SeriesNotFoundException;
import zero.ifc.SystemInterface;
import zero.lfds.LFDAlreadyExistsException;
import zero.netdispatch.DispatcherThread;
import zero.store.NotFoundException;
import zero.store.SeriesDB;
import zero.store.SeriesDefinition;
import zero.store.SeriesDefinitionDB;

public class Run {

	
	public static void main(String[] args) throws IOException, InterruptedException {

		ConfigManager.loadConfig("config.json");
		NodeDB.getInstance();				// create the NodeDB
		SeriesDefinitionDB.getInstance();	// create the SeriesDefinitionDB
		SeriesDB.getInstance();				// create the SeriesDB

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
