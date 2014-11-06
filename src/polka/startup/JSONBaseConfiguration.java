package polka.startup;

/**
 * This class has it's contents readed from a .json configuration file.
 * 
 * This is a global config class. 
 */
public class JSONBaseConfiguration {
	
	/** Address of this node's interface */
	public String node_ip;
	/** Port of this node's interface */
	public Integer node_port;
		
	/**
	 * Directory in which series data will be stored
	 */
	public String seriesdata_path;
	
	/**
	 * Directory in which series metadata will be stored
	 */
	public String seriesmeta_path;
	
	/**
	 * Amount of series to keep in memory at once
	 */
	public int series_in_memory;
	
	/*
	
}
