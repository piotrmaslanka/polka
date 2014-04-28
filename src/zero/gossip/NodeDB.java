package zero.gossip;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;

import zero.H;
import zero.startup.ConfigManager;

/**
 * Class to store information about nodes
 *
 */
public class NodeDB implements Cloneable {

	private Random random;
	
	private static NodeDB instance = null;
	
	/**
	 * Information about a single node.
	 * Immutable.
	 *
	 */
	public static class NodeInfo {
		public long nodehash;
		public InetSocketAddress nodecomms;
		
		public boolean alive;

		/**
		 * UNIX timestamp at which this data was valid
		 */
		public long timestamp;
	}

	/**
	 * All-node database
	 */
	private TreeMap<Long, NodeInfo> nodedb = new TreeMap<>();
	
	/**
	 * Updates current database with data from a gossip advertise message.
	 * 
	 * MAY call GossipThread's replicateNodeInfo() during execution
	 */
	public void update(NodeInfo[] adv) { synchronized (this.nodedb) {
		for (NodeInfo n : adv) {
			NodeInfo ni = this.nodedb.get(n.nodehash);
			if (ni != null)	// it does exist
				if (ni.timestamp >= n.timestamp)	// does not supersede this information
					continue;

			// all other cases require an update
			this.nodedb.put(n.nodehash, n);
			GossipThread.getInstance().replicateNodeInfo(n);
		}
	}}
	
	/**
	 * Get a list of nodes to gossip to, using MIRV approach
	 * TODO: This is incredibly suboptimal, because it dumps the db!
	 * @param is_propagation: was the situation due to receiving new info?
	 * 	false if the situation is a routine empty gossip 
	 */
	public NodeInfo[] getNodesToGossipTo(final boolean is_propagation) { synchronized (this.nodedb) {
		long myhash = ConfigManager.get().nodehash; 
		
		if (is_propagation) {
			Vector<NodeInfo> nv = new Vector<>();
			int i = 0;
			
			for (NodeInfo n : this.nodedb.values())
				if (n.nodehash != myhash)
					if (n.alive)
						nv.add(n);
			
			return nv.toArray(new NodeInfo[nv.size()]);
		} else {
			if (this.nodedb.size() < 2) return new NodeInfo[0];
			NodeDB.NodeInfo target = null;
			do {
				int i = this.random.nextInt(this.nodedb.size());
				target = (NodeDB.NodeInfo)(this.nodedb.values().toArray())[i];				
			} while (target.nodehash == myhash);
			return new NodeInfo[] { target };
		}
	}}
	
	/**
	 * Dump entire node into into an array
	 */
	public NodeInfo[] dump() { synchronized (this.nodedb) {
		NodeInfo[] ni = new NodeInfo[this.nodedb.size()];
		int i = 0;
		for (NodeInfo n : this.nodedb.values())
			ni[i++] = n;
		return ni;
	}}
	
	/**
	 * Initializes the DB with my own info
	 */
	private NodeDB() {
		NodeInfo me = new NodeInfo();
		me.alive = true;
		me.nodecomms = ConfigManager.get().node_interface;
		me.nodehash = ConfigManager.get().nodehash;
		me.timestamp = System.currentTimeMillis();
		this.update(new NodeDB.NodeInfo[] { me });
		this.random = new Random();
		this.random.setSeed(System.currentTimeMillis());
	}
	
	/**
	 * Get full address by node's IP address
	 * @param a IP address to locate the node by
	 * @return target address of node, null if not found
	 */
	public NodeDB.NodeInfo getNodeByInetAddress(InetAddress a) { synchronized (this.nodedb) {
		for (NodeInfo ni : this.nodedb.values()) {
			if (ni.nodecomms.getAddress().equals(a))
				return ni;
		}
		return null;
	}}
	
	public static NodeDB getInstance() {
		if (NodeDB.instance == null) NodeDB.instance = new NodeDB();
		return NodeDB.instance;
	}

	/**
	 * When it has been determined that given node has changed it's alive status
	 * 
	 * Or maybe not so much changed as just obtained some infp
	 * 
	 * @param nodehash Hash of the node that change pertains to
	 * @param b new 'alive' status
	 */
	public void onAlivenessChange(long nodehash, boolean b) { synchronized (this.nodedb) {
		NodeDB.NodeInfo n = this.nodedb.get(nodehash);
		if (n == null) return;
		if (n.alive == b) return;

		n.timestamp = System.currentTimeMillis();
		n.alive = b;
		
		GossipThread.getInstance().replicateNodeInfo(n);
	}}
	
	
	/**
	 * Returns the node responsible for this hash
	 * @param hash hash
	 * @return responsible node info
	 */
	public NodeDB.NodeInfo getResponsibleNode(long hash) { synchronized (this.nodedb) {
		for (NodeDB.NodeInfo ni : this.nodedb.values())
			if (hash <= ni.nodehash)
				return ni;
		throw new RuntimeException("Should not happen");
	}}
	
	/**
	 * Return all nodes responsible for given series
	 */
	public NodeDB.NodeInfo[] getResponsibleNodes(String name, int replicas) {
		NodeDB.NodeInfo[] ls = new NodeDB.NodeInfo[replicas];
		for (int i=0; i<replicas; i++)
			ls[i] = this.getResponsibleNode(H.hash(name, i));
		return ls;		
	}
	
	/**
	 * Return node responsible for n-th replica
	 * @param name name of the series
	 * @param rep no of the replica (0-indexed)
	 */
	public NodeDB.NodeInfo getResponsibleNode(String name, int rep) {
		return this.getResponsibleNode(H.hash(name, rep));
	}

	
}