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

	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		return SeriesDefinitionDB.getInstance().getSeries(seriesname);
	}

	@Override
	public void writeSeries(String seriesname, long prev_timestamp,	long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException {
		SeriesController sc;
		try {
			sc = SeriesDB.getInstance().getSeries(seriesname);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		try {
			sc.write(prev_timestamp, cur_timestamp, data);
		} catch (IllegalArgumentException e) {
			throw e;
		} finally {
			sc.close();
		}
	}

	@Override
	public long getHeadTimestamp(String seriesname) throws LinkBrokenException, IOException, SeriesNotFoundException {
		SeriesController sc;
		try {
			sc = SeriesDB.getInstance().getSeries(seriesname);
		} catch (NotFoundException e) {
			throw new SeriesNotFoundException();
		}
		try {
			return sc.getHeadTimestamp();
		} catch (IllegalArgumentException e) {
			throw e;
		} finally {
			sc.close();
		}
	}

	@Override
	public void updateDefinition(SeriesDefinition sd)
			throws LinkBrokenException, IOException {
		SeriesDB.getInstance().redefineAsync(sd).lock();
	}





}
