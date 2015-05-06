package mil.nga.giat.geowave.format.landsat8;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;

import org.apache.commons.cli.ParseException;

public class Landsat8DownloadCLIDriver implements
		CLIOperationDriver
{
	private final String operation;

	public Landsat8DownloadCLIDriver(
			final String operation ) {
		this.operation = operation;
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		// TODO Auto-generated method stub

	}

}
