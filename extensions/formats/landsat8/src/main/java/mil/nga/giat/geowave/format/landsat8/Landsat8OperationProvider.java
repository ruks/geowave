package mil.nga.giat.geowave.format.landsat8;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

public class Landsat8OperationProvider implements
		CLIOperationProviderSpi
{
	private static final CLIOperation[] LANDSAT8_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"analyzelandsat8",
				"Print out basic aggregate statistics for available Landsat 8 imagery",
				new Landsat8AnalyzeCLIDriver(
						"analyzelandsat8")),
		new CLIOperation(
				"downloadlandsat8",
				"Download Landsat 8 imagery to a local directory",
				new Landsat8DownloadCLIDriver(
						"downloadlandsat8")),
		new CLIOperation(
				"localingestlandsat8",
				"Ingest routine for locally downloading Landsat 8 imagery and ingesting it into GeoWave",
				new Landsat8LocalIngestCLIDriver(
						"localingestlandsat8")),
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
