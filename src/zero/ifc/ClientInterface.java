package zero.ifc;

import java.io.IOException;

import zero.gossip.NodeDB;
import zero.store.SeriesDefinition;
import zero.H;

/**
 * First point of contact for connecting clients. Serves their requests
 * that may refer to arbitrary nodes
 *
 */
public class ClientInterface implements SystemInterface {
	
	public void writeSeries(SeriesDefinition serdef, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IllegalArgumentException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(serdef.seriesName, serdef.replicaCount);

		int successes = 0;
		
		for (NodeDB.NodeInfo ni : nodes) {
			try {
				InterfaceFactory.getInterface(ni).writeSeries(serdef, prev_timestamp, cur_timestamp, data);
				successes++;
			} catch (LinkBrokenException | IOException e) {
				//
			} catch (DefinitionMismatchException | SeriesNotFoundException e) {
				throw e;	// someone has newer than we do
			} catch (IllegalArgumentException e) {
				throw e;
			}
		}
		
		if (successes == 0)  throw new LinkBrokenException();	// nobody home
	}

	public long getHeadTimestamp(SeriesDefinition serdef) throws LinkBrokenException, IOException, DefinitionMismatchException, SeriesNotFoundException {
		NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(serdef.seriesName, serdef.replicaCount);
		
		long max_head = -1;
		
		int successes = 0;
		
		for (NodeDB.NodeInfo ni : nodes) {
			try {
				long head = InterfaceFactory.getInterface(ni).getHeadTimestamp(serdef);
				successes++;
				if (head > max_head) max_head = head;
			} catch (LinkBrokenException | IOException e) {
				// pass
			} catch (DefinitionMismatchException e) {
				throw new DefinitionMismatchException();	// someone has a newer definition than we have!
			} catch (SeriesNotFoundException e) {
				throw new SeriesNotFoundException();
			}
		}
		
		if (successes == 0) throw new LinkBrokenException();	// nobody home
		return max_head;
	}
	
	
	
	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {		
		int replica_no = 0;
		long current_gen = -1;
		boolean did_somebody_answer = false;
		SeriesDefinition best_known_one = null;
		
		while (replica_no < 8) {
			long target_hash = H.hash(seriesname, replica_no);
			NodeDB.NodeInfo node_responsible = NodeDB.getInstance().getResponsibleNode(target_hash);
			if (!node_responsible.alive) continue;
			SystemInterface ifc = InterfaceFactory.getInterface(node_responsible);
			try {
				SeriesDefinition sdc = ifc.getDefinition(seriesname);
				did_somebody_answer = true;				
				if (sdc == null) continue;
				
				if (current_gen < sdc.generation) {
					best_known_one = sdc;
					current_gen = sdc.generation;
				}
			} catch (LinkBrokenException exc) {
				continue;
			} finally {
				replica_no++;
			}
		}
		
		if (!did_somebody_answer) throw new LinkBrokenException();
		
		return best_known_one;
	}	
	
	public void updateDefinition(SeriesDefinition sd) throws LinkBrokenException, IOException {
		boolean any_update_succeeded = false;
		
		for (int replica_no = 0; replica_no < sd.replicaCount; replica_no++) {
			long target_hash = H.hash(sd.seriesName, replica_no);
			NodeDB.NodeInfo node_responsible = NodeDB.getInstance().getResponsibleNode(target_hash);
			if (!node_responsible.alive) continue;			
			SystemInterface ifc = InterfaceFactory.getInterface(node_responsible);
			try {
				ifc.updateDefinition(sd);
			} catch (LinkBrokenException | IOException e) {
				// they will fault recover later
				continue;
			}
			
			any_update_succeeded = true;
		}
		
		if (!any_update_succeeded) throw new LinkBrokenException();	// nobody answered
	}

	@Override
	public void close() throws IOException {}
	
	
}