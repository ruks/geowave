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
				"ingestlandsat8",
				"Ingest routine for ingesting Landsat 8 imagery that has already been downloaded locally",
				new Landsat8IngestCLIDriver(
						"ingestlandsat8")),
		new CLIOperation(
				"downloadingestlandsat8",
				"Ingest routine for downloading and ingesting Landsat 8 imagery publicly available on AWS",
				new Landsat8IngestCLIDriver(
						"downloadingestlandsat8")),
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
