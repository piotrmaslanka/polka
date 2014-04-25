package zero.store;

import java.io.Serializable;

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
	 * Amount of replicas in the system
	 */
	public int replicaCount;
	
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
	 * Timestamp at which the series was deleted.
	 * 
	 * 0 if this has not yet been deleted
	 */
	public long tombstonedOn;
	
	/**
	 * Identifier of the series generation. If a generation
	 * changes, that means a series was deleted and remade new
	 */
	public long generation;
	
	public SeriesDefinition(String name, int recordSize, int replicaCount, String options) {
		this.seriesName = name;
		this.recordSize = recordSize;
		this.replicaCount = replicaCount;
		this.options = options;
		this.tombstonedOn = 0;
		this.generation = 0;
		this.autoTrim = 0;
	}
	
	/**
	 * Checks whether sd is more important than this definition
	 * by comparing generation
	 * @param sd a definition that is supposed to be more important than this one
	 * @return whether sd is more current/important than this definition
	 */
	public boolean doesSupersede(SeriesDefinition sd) {
		return sd.generation > this.generation;
	}
	
}
