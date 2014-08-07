package polka.store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import polka.store.SeriesDefinition;

/**
 * A particular series definition
 *
 */
public class SeriesDefinition implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Name of the series
	 */
	public String seriesName;
	
	/**
	 * Upon a write, a trim() with argument of write_timestamp - autoTrim will be issued.
	 * Set to 0 to disable this functionality
	 */
	public long autoTrim;
	
	/**
	 * Size of a record
	 */
	public int recordSize;

	/**
	 * Options passed to underlying storage
	 */
	public String options;
	
	/**
	 * Used for deleting series 
	 */
	public transient boolean isDeleted = false;
	
	/**
	 * Timestamp at which the series was deleted.
	 * 
	 * 0 if this has not yet been deleted
	 */
	public SeriesDefinition(String name, int recordSize, String options) {
		this.seriesName = name;
		this.recordSize = recordSize;
		this.options = options;
		this.autoTrim = 0;
	}
	
	// ------------------------------------ INTP representation
	public void toDataStreamasINTPRepresentation(DataOutputStream dos) throws IOException {
		dos.writeInt(recordSize);
		dos.writeLong(autoTrim);
		dos.writeUTF(options);
		dos.writeUTF(seriesName);
	}
	
	public static SeriesDefinition fromDataStreamasINTPRepresentation(DataInputStream dis) throws IOException {
		int recordSize = dis.readInt();
		long autotrim = dis.readLong();
		String options = dis.readUTF();
		String seriesName = dis.readUTF();
		
		SeriesDefinition nd = new SeriesDefinition(seriesName, recordSize, options);
		nd.autoTrim = autotrim;
		return nd;
	}		
}
