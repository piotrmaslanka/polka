package zero.startup;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import zero.gossip.GossipOutbound;
import zero.gossip.GossipThread;
import zero.gossip.NodeDB;
import zero.gossip.messages.GossipAdvertise;
import zero.ifc.LinkBrokenException;
import zero.ifc.LocalInterface;
import zero.ifc.SeriesNotFoundException;
import zero.ifc.SystemInterface;
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
		
		SystemInterface ifc = new LocalInterface();
			

		
		try {
			SeriesDefinition sd = new SeriesDefinition("miley", 4, 1, "");
			ifc.updateDefinition(sd);
			
			long timestamp = ifc.getHeadTimestamp("miley");
			byte[] dupa = new byte[]{0, 0, 0, 0};
			
			long msta = System.currentTimeMillis();			
			for (int i=0; i<10000; i++) {
				ifc.writeSeries("miley", timestamp, timestamp+1, dupa);
				timestamp++;
			}

			msta = System.currentTimeMillis() - msta;
			System.out.format("Took %f seconds\n", msta / 1000.0);
			
			
		} catch (LinkBrokenException e) {
			System.out.println("Link broken. This should never happen with LocalInterface.");
		} catch (SeriesNotFoundException e) {
			System.out.println("SNF. This should never happen with reviewed code.");
		}

		/**
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
		} **/
	}
}
