INTP:

	first, client writes byte(2) for local interface
						 byte(3) for client interface !!


		getDefinition:
		
		
					Input -> byte(0)
							 utf(series_name)
							 
					Output -> byte(0: OK, 1: IOException, 2: definition is null)
							if 0 {
								seriesDefinition(definition)
							}
							
		updateDefinition:
		
					Input -> byte(1)
							 seriesDefinition(sd)
							 
					Output -> byte(0: OK, 1: IOException)
					
		getHeadTimestamp:
		
					Input -> byte(2)
							 seriesDefinition(sd)
							 
					Output -> byte(0: OK, 1: IOException, 2: SeriesNotFound, 3: DefinitionMismatch)
							  if 0 {
							  	long headTimestamp
							  }
							  
		writeSeries:
					Input -> byte(3)
							 seriesDefinition(sd)
							 long(prev timestamp)
							 long(cur timestamp)
							 int(data length)
							 binary(data)
							 
					Output -> byte(0: OK, 1: IOException, 2: SeriesNotFound, 3: DefinitionMismatch, 4: IllegalArgumentException)