package zero.ifc;

import java.io.IOException;

import zero.store.SeriesDB;
import zero.store.SeriesDefinition;

/**
 * Resolves the calls using a local interface.
 * 
 * It means that those calls are meant for this node
 * 
 */
public class LocalInterface implements SystemInterface {

	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		return SeriesDB.getInstance().getSeries(seriesname);
	}

	@Override
	public void writeSeries(String seriesname, long prev_timestamp,	long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException {
		return SeriesDB.getInstance().
	}

	@Override
	public long getHeadTimestamp(String seriesname) throws LinkBrokenException,
			IOException, SeriesNotFoundException {
		// TODO Auto-generated method stub
		return 0;
	}





}
