package mil.nga.giat.geowave.format.landsat8;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Landsat8DownloadCommandLineOptions
{
	private static final String OVERWRITE_IF_EXISTS = "overwrite";
	private final boolean overwriteIfExists;

	public Landsat8DownloadCommandLineOptions(
			final boolean overwriteIfExists ) {
		this.overwriteIfExists = overwriteIfExists;
	}

	public static Landsat8DownloadCommandLineOptions parseOptions(
			final CommandLine commandLine ) {
		final boolean overwriteIfExists = commandLine.hasOption(OVERWRITE_IF_EXISTS);
		return new Landsat8DownloadCommandLineOptions(
				overwriteIfExists);
	}

	public static void applyOptions(
			final Options allOptions ) {

		final Option overwriteImageFiles = new Option(
				OVERWRITE_IF_EXISTS,
				false,
				"An option to overwrite images that are ingested in the local workspace directory.  By default it will keep an existing image rather than downloading it again.");
		allOptions.addOption(overwriteImageFiles);
	}

	public boolean isOverwriteIfExists() {
		return overwriteIfExists;
	}
}
