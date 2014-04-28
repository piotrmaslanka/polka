package zero.store;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import zero.lfds.LFDAlreadyExistsException;
import zero.startup.ConfigManager;

/**
 * A complex manager of all series.
 * Handles both the data storage aspect, write-ahead and metadata management
 * Singleton.
 */
public class SeriesDB {

	/**
	 * Stores currently loaded controllers.
	 * 
	 * Right now relevant entries DO NOT EXIST if there is refcount=0,
	 * but it may change as caching gets implemented
	 */
	final private Map<String, SeriesController> controllers = new HashMap<>();
	final private Map<String, Integer> refcount = new HashMap<>();

	/**
	 * Stores current updates that are to be applied to controllers
	 * 
	 * Relevant entries DO NOT EXIST if there is nothing to update
	 */
	final private Map<String, Deque<SeriesDefinition>> definition_updates = new HashMap<>();
	final private Map<String, Deque<Lock>> definition_update_completion_locks = new HashMap<>();
	
	/**
	 * Returns a SeriesController for given series
	 * @param name series name
	 * @return SeriesController for this one
	 * @throws NotFoundException this series just plain doesn't exist 
	 */
	public SeriesController getSeries(String name) throws NotFoundException, IOException {
		synchronized (this.refcount) {
			Integer refc = this.refcount.get(name);
			if (refc == null) {
				refc = 1;
				this.controllers.put(name, new SeriesController(name));
			} else
				refc++;
			this.refcount.put(name, refc);			
		}
		
		return this.controllers.get(name);
	}
	
	/**
	 * Order an asynchronous redefinition of a series.
	 * 
	 * This will return immediately.
	 * 
	 * If you want a synchronous version, .lock() on lock returned
	 * 
	 * @param sd new series definition to apply
	 * @return lock you can use to detect when operation was completed
	 */
	public Lock redefineAsync(SeriesDefinition sd) throws IOException {	
		Lock lock = new ReentrantLock();
		lock.lock();
		
		synchronized (this.refcount) {
			Deque<Lock> locklist = null;
			Deque<SeriesDefinition> sdlist = null;
			
			if (this.definition_updates.get(sd.seriesName) == null) {
				// Create new entries..
				locklist = new LinkedList<>();
				sdlist = new LinkedList<>();

				this.definition_updates.put(sd.seriesName, sdlist);
				this.definition_update_completion_locks.put(sd.seriesName, locklist);
				
			} else {
				// Just append
				locklist = this.definition_update_completion_locks.get(sd.seriesName);
				sdlist = this.definition_updates.get(sd.seriesName);
			}
			
			sdlist.addLast(sd);
			locklist.addLast(lock);			
			
			this.runRedefinitionsOrDont(sd.seriesName);
		}
		
		return lock;
	}
	

	
	/**
	 * Runs all scheduled redefinitions for given series, or doesn't - if the series is open :)
	 * 
	 * Please run synchronized on (this.refcount)
	 * 
	 * @param seriesName
	 */
	private void runRedefinitionsOrDont(String seriesName) throws IOException {
		// should be update the metadata?
		if (this.definition_update_completion_locks.get(seriesName) != null) {
			// we'll be updating..
			
			DefinitionUpdateToolkit dut = new DefinitionUpdateToolkit(seriesName);
			
			Deque<Lock> lockvector = this.definition_update_completion_locks.remove(seriesName);
			Deque<SeriesDefinition> sd = this.definition_updates.remove(seriesName);
			
			while (lockvector.size() > 0) {
				Lock lock = lockvector.pollFirst();
				SeriesDefinition newsd = sd.pollFirst();
				
				dut.changeTo(newsd);
				if (lock != null)
					lock.unlock();
			}					
		}		
	}
	
	// -------------- callbacks
	/**
	 * Called by series controller when it's successfully closed
	 * @param sc series controller that was closed
	 * @throws IllegalStateException not everything in this series has been closed 
	 */
	protected void onSeriesControllerClosed(SeriesController sc) throws IllegalStateException, IOException {
		
		String seriesName = sc.series.seriesName;		
		
		synchronized (this.refcount) {			
			Integer refc = this.refcount.get(seriesName);
			if (refc == 1) {
				// destroy the controller
				sc.physicalClose();
				this.refcount.remove(seriesName);
				this.controllers.remove(seriesName);
				
				// run redefinitions if needed
				this.runRedefinitionsOrDont(seriesName);

			} else {
				// Just decrease the refcount
				this.refcount.put(seriesName, refc-1);
			}
		}
	}
	
	// -------------- singletonish code
	private static SeriesDB instance;
	public static SeriesDB getInstance() {
		if (SeriesDB.instance == null) SeriesDB.instance = new SeriesDB();
		return SeriesDB.instance;
	}
	
	
}
