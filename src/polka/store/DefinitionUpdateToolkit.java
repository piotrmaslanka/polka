package polka.store;

import java.io.IOException;

import org.apache.commons.io.FileUtils;

import polka.lfds.LFDAlreadyExistsException;
import polka.lfds.LFDNotFoundException;
import polka.lfds.LFDOpenedException;
import polka.startup.ConfigManager;

/**
 * Class with helpers to change definitions of particular series.
 * 
 * Use it only with unused (fully closed) series.
 * Can be used with non-existent series to define them!
 */
final public class DefinitionUpdateToolkit {
	private SeriesDefinition sd;

	protected DefinitionUpdateToolkit(String seriesName) throws IOException {
		this.sd = SeriesDefinitionDB.getInstance().getSeries(seriesName);
	}
	
	/**
	 * Change to a new definition
	 * @param sd New definition
	 */
	protected void changeTo(SeriesDefinition sd) throws IOException {
		boolean performDeletion = false;
		boolean performCreation = false;
		
		if (this.sd == null) {
			performCreation = true;
			performDeletion = false;
		} else {
			// Does current series physical data need to be wiped?
			if (sd.recordSize != this.sd.recordSize) performDeletion = true;
			if (sd.options != this.sd.options) performDeletion = true;			
		}
				
		// Apply operations...
		if (performDeletion) {
			try {
				ConfigManager.get().storage.deleteSeries(sd.seriesName);				
			} catch (LFDNotFoundException e) {				 
			} catch (LFDOpenedException e) {
				throw new RuntimeException("Invariant broken: this should be closed!");
			}			
		}
		
		if (performCreation)
			try {
				ConfigManager.get().storage.createSeries(sd.seriesName, sd.recordSize, sd.options).close();
			} catch (LFDAlreadyExistsException e) {
				throw new RuntimeException("Invariant broken: this does exist!");
			} catch (LFDOpenedException e) {
				// ain't gonna happen
			}

		if (!performCreation && performDeletion)
			SeriesDefinitionDB.getInstance().deleteSeries(sd.seriesName);
		else
			SeriesDefinitionDB.getInstance().updateSeries(sd);
		
		this.sd = sd;
	}
}
