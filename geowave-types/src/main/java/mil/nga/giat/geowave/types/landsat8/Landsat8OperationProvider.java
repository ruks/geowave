package mil.nga.giat.geowave.types.landsat8;

import mil.nga.giat.geowave.ingest.CLIOperation;
import mil.nga.giat.geowave.ingest.CLIOperationCategory;
import mil.nga.giat.geowave.ingest.CLIOperationProviderSpi;

public class Landsat8OperationProvider implements
		CLIOperationProviderSpi
{
	private static final CLIOperation[] LANDSAT8_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"analyze-landsat8",
				"Print out basic aggregate statistics for available Landsat 8 imagery",
				new Landsat8IngestCLIDriver(
						"analyze-landsat8")),
		new CLIOperation(
				"download-landsat8",
				"Download Landsat 8 imagery to a local directory",
				new Landsat8IngestCLIDriver(
						"download-landsat8")),
		new CLIOperation(
				"ingest-landsat8",
				"Ingest routine for ingesting Landsat 8 imagery that has already been downloaded locally",
				new Landsat8IngestCLIDriver(
						"ingest-landsat8")),
		new CLIOperation(
				"download-ingest-landsat8",
				"Ingest routine for downloading and ingesting Landsat 8 imagery publicly available on AWS",
				new Landsat8IngestCLIDriver(
						"ingest-landsat8")),
	};

	@Override
	public CLIOperation[] getOperations() {
		return LANDSAT8_OPERATIONS;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return new Landsat8OperationCategory();
	}

}
