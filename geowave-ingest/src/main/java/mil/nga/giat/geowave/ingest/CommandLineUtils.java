package mil.nga.giat.geowave.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommandLineUtils
{
	public static Option getHelpOption() {
		return new Option(
				"h",
				"help",
				false,
				"Display help");
	}

	public static void parseHelpOption(
			CommandLine commandLine,
			Options options,
			String operation ) {
		if (commandLine.hasOption("h")) {
			printHelp(
					options,
					operation);
			System.exit(0);
		}
	}

	public static void printHelp(
			final Options options,
			final String operation ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"-" + operation + " <options>",
				"\nOptions:",
				options,
				"");
	}
}
