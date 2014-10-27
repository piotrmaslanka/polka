package polka.store;

import java.io.Closeable;
import java.io.IOException;

import polka.lfds.LFDDamagedException;
import polka.lfds.LFDNotFoundException;
import polka.lfds.LFDOpenedException;
import polka.lfds.LFDResultSet;
import polka.lfds.LFDSeries;
import polka.startup.ConfigManager;

/**
 * This is a thread-safe singleton-per-series
 * This represents a single series existing in the system, both the data and recovery aspect
 *
 */
public class SeriesController implements Closeable {
	
	protected SeriesDefinition series;	// contains info about the series
	private LFDSeries primary_storage;			// primary storage	
	
	/**
	 * Returns operational head timestamp.
	 * 
	 * This is the timestamp up to which registered data
	 * is contiguous. -1 if empty.
	 *
	 * @return operational head timestamp
	 */
	public long getHeadTimestamp() {
		return this.primary_storage.getHeadTimestamp();
	}
	
	/**
	 * Returns a series definition for underlying series
	 * @return series definition for this one
	 */
	public SeriesDefinition getSeriesDefinition() {
		return this.series;
	}
	
	/**
	 * Creates a SeriesController for given series
	 * @param name name of the series
	 * @throws NotFoundException thrown when series is not found (tombstoned or not declared)
	 */
	public SeriesController(String name) throws NotFoundException, IOException {
		this.series = SeriesDefinitionDB.getInstance().getSeries(name);
		if (this.series == null) throw new NotFoundException();

		try {
			this.primary_storage = ConfigManager.get().storage.getSeries(name);
		} catch (LFDNotFoundException | LFDDamagedException e) {
			// if there is a definition but not a LFD allocation then something's wrong
			throw new IOException();
		}		
	}
	
	/**
	 * Adds an entry
	 * @param currentTimestamp timestamp of this write
	 * @param value value to write to DB
	 * @throws IllegalArgumentException value passsed was malformed
	 */
	public synchronized void write(long currentTimestamp, byte[] value) throws IllegalArgumentException, IOException {
		
		long rootserTimestamp = this.primary_storage.getHeadTimestamp();
		if (currentTimestamp <= rootserTimestamp) return;	// just ignore this write
		
		this.primary_storage.write(currentTimestamp, value);
 			
		if (this.series.autoTrim > 0)
			this.primary_storage.trim(currentTimestamp - this.series.autoTrim);
	}
	
	/**
	 * Reads data from the series
	 */
	public LFDResultSet read(long from, long to) throws IOException {
		try {
			return this.primary_storage.read(from, to);
		} catch (LFDDamagedException e) {
			throw new IOException();
		}
	}
	
	/**
	 * Signals that this SeriesController is no longer used by the one that 
	 * got it from SeriesDB 
	 */
	@Override
	public void close() throws IOException {
		SeriesDB.getInstance().onSeriesControllerClosed(this);		
	}
	
	/**
	 * Closes the controller
	 * 
	 * Called by SeriesDB when it wants to close the controller
	 * 
	 * @throws IllegalStateException not all result sets were closed
	 */
	protected void physicalClose() throws IOException, IllegalStateException {
		try {
			this.primary_storage.close();
		} catch (LFDOpenedException e) {
			throw new IllegalStateException();
		}
	}
	
	/**
	 * Deletes primary and partial storages.
	 * Controller must be closed.
	 */
	protected void deleteStorages() throws IOException {
		try {
			ConfigManager.get().storage.deleteSeries(this.series.seriesName);
		} catch (LFDNotFoundException | LFDOpenedException e) {
			throw new IOException("Failed to delete!");
		}
	}
	
}
