package polka.ifc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import polka.lfds.LFDDamagedException;
import polka.lfds.LFDResultSet;
import polka.store.NotFoundException;
import polka.store.SeriesController;
import polka.store.SeriesDB;
import polka.store.SeriesDefinition;
import polka.store.SeriesDefinitionDB;

public class LocalInterface {

	/**
	 * Obtains a definition of a given series.
	 * 
	 * @param name name of the series
	 * @return definition of given series, or null if it doesn't exist
	 */
	public SeriesDefinition getDefinition(String seriesname) throws IOException {
		return SeriesDefinitionDB.getInstance().getSeries(seriesname);
	}

	/**
	 * Write a value to target series
	 * @param name series name
	 * @param cur_timestamp timestamp of this record
	 * @param data data to write
	 * @throws IllegalArgumentException timestamps invalid or invalid data
	 * @throws SeriesNotFoundException when a series does not exist (and write was pointless because of that)
	 * @throws DefinitionMismatchException if generation doesn't match (and caller provided an older one)
	 */
	public void writeSeries(String name, long cur_timestamp, byte[] data) throws IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		SeriesController ctrl = null;
		try {
			ctrl = SeriesDB.getInstance().getSeries(name);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		
		if (ctrl.getSeriesDefinition().recordSize != data.length) throw new IllegalArgumentException("Data size wrong");
		
		try {
			ctrl.write(cur_timestamp, data);
		} finally {
			ctrl.close();
		}
		
	}

	/**
	 * Gets target series' operational head (up to this head the series is readable).
	 * Series may contain newer data in it's Write-Ahead, but it may not be readable.
	 * -1 when series is empty.
	 * 
	 * @param name name of target series
	 * @return series' head
	 * @throws SeriesNotFoundException when a series does not exist (so it can't be provided)
	 * @throws DefinitionMismatchException if generation doesn't match (and caller provided an older one)
	 */
	public long getHeadTimestamp(String name) throws IOException, SeriesNotFoundException, DefinitionMismatchException {
		SeriesController ctrl = null;
		try {
			ctrl = SeriesDB.getInstance().getSeries(name);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		try {
			return ctrl.getHeadTimestamp();
		} finally {
			ctrl.close();
		}
	}

	/**
	 * Change a definition of a given series.
	 * Supplants current definition with this new definition. It will happen only
	 * if generation numbering is right (new gen is larger than current node's gen).
	 * 
	 * This will block until update hits the DB.
	 * 
	 * @param sd SeriesDefinition. CANNOT be null.
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 */
	public void updateDefinition(SeriesDefinition sd) throws IOException {
		SeriesDB.getInstance().redefineAsync(sd).lock();
	}

	/**
	 * Read data into target output stream.
	 * 
	 * Format is
	 * 	an array of records (8byte big endian TIMESTAMP, binary[recordsize] data)
	 * 	8 byte big endian -1
	 * 
	 * @param name name of target series
	 * @param from start timestamp
	 * @param to stop timestamp
	 * @param channel target channel
	 */	
	public void read(String name, long from, long to, WritableByteChannel channel)
		throws IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException {
		
		SeriesController ctrl = null;
		try {
			ctrl = SeriesDB.getInstance().getSeries(name);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		
		try {
			LFDResultSet rs = null;
			try {
				rs = ctrl.read(from, to);
				// allocate buffers for 1024 entries
				ByteBuffer bb = ByteBuffer.allocateDirect(1024*(ctrl.getSeriesDefinition().recordSize+8));
				
				while (!rs.isFinished()) {
					bb.clear();
					try {
						rs.fetch(bb, 1024);
					} catch (LFDDamagedException e) {
						throw new IOException();
					}
					bb.flip();
					channel.write(bb);
				}
				
				bb.clear();
				bb.putLong(-1);
				bb.flip();
				channel.write(bb);
			} finally {
				rs.close();
			}
		} finally {
			ctrl.close();
		}		
		
	}

	public void close() throws IOException {}


	/**
	 * Gets target series' operational head and included data.
	 * Think of it as a read() that returns only data specified by getHeadTimestamp()
	 * 
	 * Return format is
	 * 	record (8byte big endian TIMESTAMP, binary[recordsize] data) if head != -1
	 * 	8 byte big endian -1 
	 * 
 	 * @param name name of target series
	 * @param channel target channel
	 */
	public void readHead(String name, WritableByteChannel channel)
			throws IOException, SeriesNotFoundException,
			DefinitionMismatchException {

		SeriesController ctrl = null;
		try {
			ctrl = SeriesDB.getInstance().getSeries(name);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		
		try {
			LFDResultSet rs = null;
			try {
				long head = ctrl.getHeadTimestamp();
				rs = ctrl.read(head, Long.MAX_VALUE);
				// allocate buffers for 1 entry
				ByteBuffer bb = ByteBuffer.allocateDirect(ctrl.getSeriesDefinition().recordSize+8);
				
				if (head != -1) {
					try {
						rs.fetch(bb, 1);
					} catch (LFDDamagedException e) {
						throw new IOException();
					}
					bb.flip();
					channel.write(bb);
				}
				
				bb.clear();
				bb.putLong(-1);
				bb.flip();
				channel.write(bb);
			} finally {
				rs.close();
			}
		} finally {
			ctrl.close();
		}						
	}
}
