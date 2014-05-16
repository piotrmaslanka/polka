package zero.ifc;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import zero.gossip.NodeDB;
import zero.store.SeriesDefinition;
import zero.H;

/**
 * First point of contact for connecting clients. Serves their requests
 * that may refer to arbitrary nodes
 *
 */
public class ClientInterface implements SystemInterface {
	
	private final static int MAX_REPLICAS = 4;
	
	public void writeSeries(SeriesDefinition serdef, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IllegalArgumentException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(serdef.seriesName, serdef.replicaCount);

		int successes = 0;
		
		for (NodeDB.NodeInfo ni : nodes) {
			if (!ni.alive) continue;
	
			SystemInterface sin = null;
			try {
				sin = InterfaceFactory.getInterface(ni);
			} catch (IOException e) {
				continue;
			}
			
			try {
				sin.writeSeries(serdef, prev_timestamp, cur_timestamp, data);
				successes++;
			} catch (LinkBrokenException | IOException e) {
				//
			} catch (DefinitionMismatchException | SeriesNotFoundException e) {
				throw e;	// someone has newer than we do
			} catch (IllegalArgumentException e) {
				throw e;
			} finally {
				sin.close();
			}
		}
		
		if (successes == 0)  throw new LinkBrokenException();	// nobody home
	}

	public long getHeadTimestamp(SeriesDefinition serdef) throws LinkBrokenException, IOException, DefinitionMismatchException, SeriesNotFoundException {
		NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(serdef.seriesName, serdef.replicaCount);
		
		long max_head = -1;
		
		int successes = 0;
		
		for (NodeDB.NodeInfo ni : nodes) {
			if (!ni.alive) continue;
			
			SystemInterface sin = null;
			try {
				sin = InterfaceFactory.getInterface(ni);
			} catch (IOException e) {
				continue;
			}
			
			try {
				long head = sin.getHeadTimestamp(serdef);
				successes++;
				if (head > max_head) max_head = head;
			} catch (LinkBrokenException | IOException e) {
				// pass
			} catch (DefinitionMismatchException e) {
				throw new DefinitionMismatchException();	// someone has a newer definition than we have!
			} catch (SeriesNotFoundException e) {
				throw new SeriesNotFoundException();
			} finally {
				sin.close();
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
		
		while (replica_no < MAX_REPLICAS) {
			long target_hash = H.hash(seriesname, replica_no);
			NodeDB.NodeInfo node_responsible = NodeDB.getInstance().getResponsibleNode(target_hash);
			if (!node_responsible.alive) continue;
			
			SystemInterface ifc = null;
			try {
				ifc = InterfaceFactory.getInterface(node_responsible);
			} catch (IOException e) {
				replica_no++;
				continue;
			}					
			
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
				ifc.close();
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

			SystemInterface ifc = null;
			try {
				ifc = InterfaceFactory.getInterface(node_responsible);
			} catch (IOException e) {
				continue;
			}		
			
			try {
				ifc.updateDefinition(sd);
				any_update_succeeded = true;
			} catch (LinkBrokenException | IOException e) {
				// they will fault recover later
				continue;
			} finally {
				ifc.close();
			}
		}
		
		if (!any_update_succeeded) throw new LinkBrokenException();	// nobody answered
	}

	@Override
	public void close() throws IOException {}

	@Override
	public void read(SeriesDefinition sd, long from, long to,
			WritableByteChannel channel) throws LinkBrokenException,
			IOException, SeriesNotFoundException, DefinitionMismatchException,
			IllegalArgumentException {
		
		NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(sd.seriesName, sd.replicaCount);

		for (NodeDB.NodeInfo ni : nodes) {
			if (!ni.alive) continue;
			
			SystemInterface sin = null;
			try {
				sin = InterfaceFactory.getInterface(ni);
			} catch (IOException e) {
				continue;
			}			
			
			try {
				sin.read(sd, from, to, channel);
				return;
			} catch (LinkBrokenException | IOException e) {
				continue;
			} catch (DefinitionMismatchException | SeriesNotFoundException e) {
				throw e;	// someone has newer than we do
			} catch (IllegalArgumentException e) {
				throw e;
			} finally {
				sin.close();
			}
		}
		
		throw new LinkBrokenException();	// nobody home		
		
	}
	
	
}
