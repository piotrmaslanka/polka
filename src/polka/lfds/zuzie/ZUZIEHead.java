package polka.lfds.zuzie;

import java.io.IOException;
import java.nio.ByteBuffer;

import polka.lfds.LFDDamagedException;
import polka.lfds.LFDResultSet;

public class ZUZIEHead implements LFDResultSet {

	private long timestamp;
	private int recsize;
	private byte[] value;
	private boolean readed;
	
	/**
	 * Initialize the response. If timestamp is negative, then no series are here
	 */
	protected ZUZIEHead(long timestamp, byte[] value, int recsize) {
		this.timestamp = timestamp;
		this.value = value;
		this.recsize = recsize;
		this.readed = (timestamp == -1);
	}
	
	public long getStartingPosition() { return this.timestamp; }
	public long getEndingPosition() { return this.timestamp; }
	public int getRecordSize() { return this.recsize; }
	public boolean isFinished() { return this.readed; }
	
	public int fetch(long[] timestamps, ByteBuffer rawdata, int bufsize) throws IOException, LFDDamagedException {
		if (this.readed) return 0;
		if (bufsize == 0) return 0;
		timestamps[0] = this.timestamp;
		rawdata.put(this.value);
		this.readed = true;
		return 1;
	}
	
	public int fetch(ByteBuffer rawdata, int bufsize) throws IOException, LFDDamagedException {
		if (this.readed) return 0;
		if (bufsize == 0) return 0;
		rawdata.putLong(this.timestamp);
		rawdata.put(this.value);
		this.readed = true;
		return 1;		
	}
		
	public void close() throws IOException {}

}
