package zero.store;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import zero.lfds.LFDAlreadyExistsException;
import zero.startup.ConfigManager;

/**
 * A complex manager of all series.
 * Handles both the data storage aspect, write-ahead and metadata management
 * Singleton.
 * 
 * This may be extremely slow in parallel as it has a global lock
 */
public class SeriesDB {

	/**
	 * Stores currently loaded controllers
	 */
	private Map<String, SeriesController> controllers = new HashMap<>();
	
	
	
	/**
	 * Return currently functioning series definition
	 * @param name name of the series
	 * @return it's controller, or null if not found
	 */
	public SeriesController getSeries(String name) throws IOException { synchronized (this.controllers) {
		SeriesController sc = this.controllers.get(name);
		if (sc == null)
			try {
				this.controllers.put(name, new SeriesController(name));
			} catch (NotFoundException e) {
				return null;
			}
		return this.controllers.get(name);
	}}
	
	/**
	 * Redefines a series 
	 * @param sd new definition
	 */
	public void defineSeries(SeriesDefinition sd) throws IOException {
		while (true) {			
			synchronized (this.controllers) {
				SeriesController sc = this.controllers.get(sd.seriesName);
				if (sc == null) {
					// it's not in use, apply the redefinition
					SeriesDefinition currentsd = SeriesDefinitionDB.getInstance().getSeries(sd.seriesName);
					if (currentsd != null)
						if (!currentsd.doesSupersede(sd)) return; // not a new thing
					
					// Apply it now
					SeriesDefinitionDB.getInstance().updateSeries(sd);
					
					if (sd.tombstonedOn != 0) {
						try {
							sc = new SeriesController(sd.seriesName);
							sc.close();
							sc.deleteStorages();
						} catch (NotFoundException e) {}
						return;
					}
					
					boolean create_new = false;
					if (currentsd == null)
						create_new = true;
					else
						if (sd.tombstonedOn == 0)
							create_new = true;
					
					if (create_new)
						try {
							ConfigManager.get().storage.createSeries(sd.seriesName, sd.recordSize, sd.options);
						} catch (LFDAlreadyExistsException e) {
							throw new IOException("Already exists!");
						}
				}
			}
			Thread.yield();
		}
	}
	
	// -------------- callbacks
	/**
	 * Called by series controller when it's successfully closed
	 * @param sc series controller that was closed
	 */
	protected void onSeriesControllerClosed(SeriesController sc) {
		synchronized (this.controllers) {
			this.controllers.remove(sc.series.seriesName);
		}
	}
	
	// -------------- singletonish code
	private static SeriesDB instance;
	public static SeriesDB getInstance() {
		if (SeriesDB.instance == null) SeriesDB.instance = new SeriesDB();
		return SeriesDB.instance;
	}
	
	
}
