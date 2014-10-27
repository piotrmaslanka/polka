package polka.startup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Paths;

import polka.lfds.zuzie.ZUZIEDriver;

import com.google.gson.Gson;


/**
 * Class to return the configuration object
 *
 */
public class ConfigManager {
	
	private static BaseConfiguration instance = null;
	
	/**
	 * Returns the config
	 * @return BaseConfiguration loaded earlier
	 */
	public static BaseConfiguration get() {
		return ConfigManager.instance;
	}
	
	/**
	 * Loads config from a file and stores it for futher retrieval
	 * @param path Path to JSON config file
	 */
	static void loadConfig(String path) {
		Gson gson = new Gson();
		JSONBaseConfiguration bc = null;
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
			bc = gson.fromJson(reader, JSONBaseConfiguration.class);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Configuration file not found");
		}	
		BaseConfiguration b = new BaseConfiguration();
		
		b.node_interface = new InetSocketAddress(bc.node_ip, bc.node_port);
		
		b.series_in_memory = bc.series_in_memory;
		b.datapath = Paths.get(bc.seriesdata_path);
		b.metapath = Paths.get(bc.seriesmeta_path);
		
		b.storage = new ZUZIEDriver(b.datapath);
		
		ConfigManager.instance = b;
	}
}
