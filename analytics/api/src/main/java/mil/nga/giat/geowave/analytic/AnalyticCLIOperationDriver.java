package mil.nga.giat.geowave.analytic;

import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticCLIOperationDriver implements
		CLIOperationDriver
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticCLIOperationDriver.class);
	private final IndependentJobRunner jobRunner;
	private final String operation;

	public AnalyticCLIOperationDriver(
			final String operation,
			final IndependentJobRunner jobRunner ) {
		super();
		this.operation = operation;
		this.jobRunner = jobRunner;
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(CommandLineUtils.getHelpOption());
		options.addOptionGroup(baseOptionGroup);

		final Set<Option> optionSet = new HashSet<Option>();
		jobRunner.fillOptions(optionSet);
		for (final Option option : optionSet) {
			options.addOption(option);
		}

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		CommandLineUtils.parseHelpOption(
				commandLine,
				options,
				operation);
		final PropertyManagement pm = new PropertyManagement();
		pm.buildFromOptions(commandLine);
		try {
			jobRunner.run(pm);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run analytic job",
					e);
			return;
		}
	}
}
