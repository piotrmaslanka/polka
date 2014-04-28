package zero.ifc;

import java.io.IOException;

import zero.store.SeriesDefinition;

/**
 * Generic interface that represents interface of a ZeroDB node.
 * Consumer's don't care if this is a network face or a local node interface
 */
public interface SystemInterface {
	
	/**
	 * Change a definition of a given series.
	 * Supplants current definition with this new definition. It will happen only
	 * if generation numbering is right (new gen is larger than current node's gen).
	 * 
	 * This will block until update hits the DB.
	 * 
	 * @param sd SeriesDefinition
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 */
	public void updateDefinition(SeriesDefinition sd) throws LinkBrokenException, IOException;
	
	/**
	 * Obtains a definition of a given series
	 * @param name name of the series
	 * @return definition of given series, or null if it doesn't exist
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 */
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException;
	
	/**
	 * Write a value to target series
	 * @param seriesname name of the series to write to
	 * @param prev_timestamp timestamp of previous record
	 * @param cur_timestamp timestamp of this record
	 * @param data data to write
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 * @throws IllegalArgumentException timestamps invalid or invalid data
	 * @throws SeriesNotFoundException when a series does not exist
	 */
	public void writeSeries(String seriesname, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException;

	/**
	 * Gets target series' operational head (up to this head the series is readable).
	 * Series may contain newer data in it's Write-Ahead, but it may not be readable.
	 * -1 when series is empty.
	 * @param seriesname name of the series
	 * @return series' head
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 * @throws SeriesNotFoundException when a series does not exist
	 */
	public long getHeadTimestamp(String seriesname) throws LinkBrokenException, IOException, SeriesNotFoundException;
}
