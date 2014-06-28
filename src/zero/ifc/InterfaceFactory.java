package zero.ifc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import zero.ifc.LocalInterface;
import zero.ifc.NetworkInterface;
import zero.gossip.NodeDB;

/**
 * You pass it a NodeDB.NodeInfo, it returns you an interface. Fair'n'square
 */
public class InterfaceFactory {
	
	
	static private Map<Long, Vector<NetworkInterface>> pool = new HashMap<>();
	
	/**
	 * Returns an interface. May block
	 * @param nodeifc target node info
	 * @return interface
	 */
	static public synchronized SystemInterface getInterface(NodeDB.NodeInfo nodeifc) throws IOException {
		if (nodeifc.isLocal)
			return new LocalInterface();
		else {
			
			// lets try getting it from cache
			Vector<NetworkInterface> vni = pool.get(nodeifc.nodehash);
			if (vni == null) { vni = new Vector<NetworkInterface>(); pool.put(nodeifc.nodehash, vni); }
			
			if (vni.size() == 0) {
				// spawnve another one
				return new NetworkInterface(nodeifc.nodecomms, nodeifc); 
			} else {
				NetworkInterface a = vni.get(0);
				vni.remove(0);
				
				if (vni.size() == 0) pool.remove(nodeifc.nodehash);
				return a;
			}
		}
	}
	
	
	static public synchronized void returnConnection(NodeDB.NodeInfo ni, NetworkInterface nifc) throws IOException {
		Vector<NetworkInterface> vni = pool.get(ni.nodehash);
		if (vni == null) { vni = new Vector<NetworkInterface>(); pool.put(ni.nodehash, vni); }
		vni.add(nifc);
		returnConnectionFailed(ni, nifc);
	}
	
	/**
	 * This returns a connection with a hint that it has failed
	 */
	static public synchronized void returnConnectionFailed(NodeDB.NodeInfo ni, NetworkInterface nifc) throws IOException {
		System.out.println("Returning failed connection");
		nifc.physicalClose();
	}

}
