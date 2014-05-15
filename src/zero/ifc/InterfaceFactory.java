package zero.ifc;

import java.io.IOException;
import zero.ifc.LocalInterface;
import zero.ifc.NetworkInterface;
import zero.gossip.NodeDB;

/**
 * You pass it a NodeDB.NodeInfo, it returns you an interface. Fair'n'square
 */
public class InterfaceFactory {
	
	/**
	 * Returns an interface. May block
	 * @param nodeifc target node info
	 * @return interface
	 */
	static public SystemInterface getInterface(NodeDB.NodeInfo nodeifc) throws IOException {
		if (nodeifc.isLocal)
			return new LocalInterface();
		else
			return new NetworkInterface(nodeifc.nodecomms);
	}

}
