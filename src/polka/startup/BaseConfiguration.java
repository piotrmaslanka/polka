package polka.startup;

import java.net.InetSocketAddress;
import java.nio.file.Path;

import polka.lfds.LFDDriver;

/**
 * Base configuration info object, after being parsed from JSON
 * and filled in by ConfigManager
 *
 */
public class BaseConfiguration {
	/**
	 * this node's interface
	 */
	public InetSocketAddress node_interface;
	
	/**
	 * UNIX socket name to listen on, null if none
	 */
	public String unix_socket_name;
	
	/**
	 * Directory in which time series data should be stored
	 */
	public Path datapath;
	
	/**
	 * Directory in which time series metadata should be stored
	 */
	public Path metapath;

	public int series_in_memory;
	
	// -------------------- derived from configuration
	
	/**
	 * Storage for principal data
	 */
	public LFDDriver storage;

}