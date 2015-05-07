package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineUtils;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Landsat8IngestCLIDriver implements
		CLIOperationDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8IngestCLIDriver.class);

	private final String operation;

	public Landsat8IngestCLIDriver(
			final String operation ) {
		this.operation = operation;
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		final Options options = new Options();
		options.addOption(CommandLineUtils.getHelpOption());
		AccumuloCommandLineOptions.applyOptions(options);
		Landsat8CommandLineOptions.applyOptions(options);

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		CommandLineUtils.parseHelpOption(
				commandLine,
				options,
				operation);
		final Landsat8CommandLineOptions landsatOptions = Landsat8CommandLineOptions.parseOptions(commandLine);
		try {
			final SceneFeatureIterator scenes = new SceneFeatureIterator(
					landsatOptions.isOnlyScenesSinceLastRun(),
					landsatOptions.isUseCachedScenes(),
					landsatOptions.getCqlFilter(),
					landsatOptions.getWorkspaceDir());
		}
		catch (final IOException e) {
			LOGGER.error(
					"",
					e);
		}
	}

	private void downloadScenes() {

	}

	private void downloadGeotiffs(
			final String workspace ) {

	}
}
