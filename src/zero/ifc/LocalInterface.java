package zero.ifc;

import java.io.IOException;

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
		System.out.format("writeSeries(%s, %d, %d, ...)\n", sd.seriesName, prev_timestamp, cur_timestamp);
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
	public void close() throws IOException {}
}
