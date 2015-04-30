package mil.nga.giat.geowave.types.landsat8;

import mil.nga.giat.geowave.ingest.CLIOperation;
import mil.nga.giat.geowave.ingest.CLIOperationCategory;
import mil.nga.giat.geowave.ingest.CLIOperationProviderSpi;
import mil.nga.giat.geowave.ingest.IngestOperationCategory;

public class Landsat8IngestOperationProvider implements
		CLIOperationProviderSpi
{
	private static final CLIOperation[] LANDSAT_INGEST = new CLIOperation[] {
		new CLIOperation(
				"ingest-landsat8",
				"Ingest routine for downloading and ingesting Landsat 8 imagery publicly available on AWS",
				new Landsat8IngestCLIDriver(
						"ingest-landsat8"))
	};

	@Override
	public CLIOperation[] getOperations() {
		return LANDSAT_INGEST;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return new IngestOperationCategory();
	}

}
