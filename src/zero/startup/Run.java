package zero.startup;

import java.io.IOException;

import zero.gossip.GossipOutbound;
import zero.gossip.GossipThread;
import zero.gossip.NodeDB;
import zero.gossip.messages.GossipAdvertise;
import zero.lfds.LFDAlreadyExistsException;
import zero.netdispatch.DispatcherThread;
import zero.store.NotFoundException;
import zero.store.SeriesDB;
import zero.store.SeriesDefinition;
import zero.store.SeriesDefinitionDB;

public class Run {

	
	public static void main(String[] args) throws IOException, InterruptedException, LFDAlreadyExistsException, IllegalArgumentException, NotFoundException {

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
		
		
		while (true) {
			Thread.sleep(10000);
			System.out.println("----------------------------");
			NodeDB.NodeInfo[] nodes = NodeDB.getInstance().dump();
			for (NodeDB.NodeInfo node : nodes)
				System.out.format("%d at %s (alive=%b)\n", node.nodehash, node.nodecomms.toString(), node.alive);
		}
	}
}
