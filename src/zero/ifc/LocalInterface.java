package zero.ifc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import zero.lfds.LFDDamagedException;
import zero.lfds.LFDResultSet;
import zero.store.NotFoundException;
import zero.store.SeriesController;
import zero.store.SeriesDB;
import zero.store.SeriesDefinition;
import zero.store.SeriesDefinitionDB;

/**
 * Resolves the calls using a local interface.
 * 
 * It means that those calls are meant for this node
 * 
 */
public class LocalInterface implements SystemInterface {

	/**
	 * Opens a controller, updating the series definition if necessary
	 */
	private SeriesController openController(SeriesDefinition extsd) throws IOException, DefinitionMismatchException {
		SeriesDefinition myser = null;
		myser = SeriesDefinitionDB.getInstance().getSeries(extsd.seriesName);
		
		if (extsd.doesSupersede(myser))
			throw new DefinitionMismatchException();			// outdated
		else if (extsd.equals(myser)) {
			try {
				return SeriesDB.getInstance().getSeries(extsd.seriesName);	// just fine!
			} catch (NotFoundException exc) {
				// But we just defined that before!!!!
				throw new IOException();
			}
		} else {
			SeriesDB.getInstance().redefineAsync(extsd).lock();		// superdated
			return this.openController(extsd);
		}
	}
	
	
	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		return SeriesDefinitionDB.getInstance().getSeries(seriesname);
	}

	@Override
	public void writeSeries(SeriesDefinition sd, long prev_timestamp,	long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		SeriesController ctrl = this.openController(sd);
		if (ctrl.getSeriesDefinition().recordSize != data.length) throw new IllegalArgumentException("Data size wrong");
		
		try {
			ctrl.write(prev_timestamp, cur_timestamp, data);
		} finally {
			ctrl.close();
		}
		
	}

	@Override
	public long getHeadTimestamp(SeriesDefinition sd) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException {
		SeriesController ctrl = this.openController(sd);		
		try {
			return ctrl.getHeadTimestamp();
		} finally {
			ctrl.close();
		}
	}

	@Override
	public void updateDefinition(SeriesDefinition sd)
			throws LinkBrokenException, IOException {
		SeriesDB.getInstance().redefineAsync(sd).lock();
	}

	@Override
	public void read(SeriesDefinition sd, long from, long to, WritableByteChannel channel)
		throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException {
		
		SeriesController ctrl = this.openController(sd);		
		try {
			LFDResultSet rs = null;
			int tot_rin = 0;
			try {
				rs = ctrl.read(from, to);
				// allocate buffers for 1024 entries
				ByteBuffer bb = ByteBuffer.allocateDirect(1024*(sd.recordSize+8));
				
				while (!rs.isFinished()) {
					System.out.println("Fetch!");
					bb.clear();
					try {
						tot_rin += rs.fetch(bb, 1024);
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
				System.out.format("read(%s): %d readed in, nach (%d, %d) gefragt\n", sd.seriesName, tot_rin, from, to);
			} finally {
				rs.close();
			}
		} finally {
			ctrl.close();
		}		
		
	}

	
	@Override
	public void close() throws IOException {}
}
