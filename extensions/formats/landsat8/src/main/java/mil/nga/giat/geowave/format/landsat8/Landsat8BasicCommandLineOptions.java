package mil.nga.giat.geowave.format.landsat8;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Landsat8BasicCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8BasicCommandLineOptions.class);
	private static final String DEFAULT_WORKSPACE_DIR = "landsat8";
	private static final String WORKSPACE_DIR_OPTION = "wsdir";
	private static final String CQL_FILTER_OPTION = "cql";
	private static final String ONLY_SCENES_SINCE_LAST_RUN_OPTION = "sincelastrun";
	private static final String USE_CACHED_SCENES_OPTION = "usecachedscenes";
	private static final String N_BEST_SCENES = "nbestscenes";

	private final String workspaceDir;
	private final String cqlFilter;
	private final boolean onlyScenesSinceLastRun;
	private final boolean useCachedScenes;
	private final int nBestScenes;

	public Landsat8BasicCommandLineOptions(
			final String workspaceDir,
			final String cqlFilter,
			final boolean onlyScenesSinceLastRun,
			final boolean useCachedScenes,
			final int nBestScenes ) {
		this.workspaceDir = workspaceDir;
		this.cqlFilter = cqlFilter;
		this.onlyScenesSinceLastRun = onlyScenesSinceLastRun;
		this.useCachedScenes = useCachedScenes;
		this.nBestScenes = nBestScenes;
	}

	public static Landsat8BasicCommandLineOptions parseOptions(
			final CommandLine commandLine ) {
		String workspaceDir;
		if (commandLine.hasOption(WORKSPACE_DIR_OPTION)) {
			workspaceDir = commandLine.getOptionValue(WORKSPACE_DIR_OPTION);
		}
		else {
			workspaceDir = System.getProperty("java.io.tmpdir") + File.separator + DEFAULT_WORKSPACE_DIR;
		}
		final String cqlFilter;
		if (commandLine.hasOption(CQL_FILTER_OPTION)) {
			cqlFilter = commandLine.getOptionValue(CQL_FILTER_OPTION);
		}
		else {
			cqlFilter = null;
		}

		final int nBestScenes;
		if (commandLine.hasOption(N_BEST_SCENES)) {
			nBestScenes = Integer.parseInt(commandLine.getOptionValue(N_BEST_SCENES));
		}
		else {
			nBestScenes = -1;
		}
		final boolean onlyScenesSinceLastRun = commandLine.hasOption(ONLY_SCENES_SINCE_LAST_RUN_OPTION);
		final boolean useCachedScenes = commandLine.hasOption(USE_CACHED_SCENES_OPTION);
		return new Landsat8BasicCommandLineOptions(
				workspaceDir,
				cqlFilter,
				onlyScenesSinceLastRun,
				useCachedScenes,
				nBestScenes);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option workspaceDir = new Option(
				WORKSPACE_DIR_OPTION,
				true,
				"A local directory to write temporary files needed for landsat 8 ingest. Default is <TEMP_DIR>/landsat8");
		workspaceDir.setRequired(false);
		allOptions.addOption(workspaceDir);

		final Option cqlFilter = new Option(
				CQL_FILTER_OPTION,
				true,
				"An optional CQL expression to filter the ingested imagery. The feature type for the expression has the following attributes: shape (Geometry), acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row (int) and the feature ID is entityId for the scene");
		cqlFilter.setRequired(false);
		allOptions.addOption(cqlFilter);

		final Option nBestScenes = new Option(
				N_BEST_SCENES,
				true,
				"An option to identify and only use a set number of scenes with the best cloud cover");
		nBestScenes.setRequired(false);
		allOptions.addOption(nBestScenes);

		final Option onlyScenesSinceLastRun = new Option(
				ONLY_SCENES_SINCE_LAST_RUN_OPTION,
				false,
				"An option to check the scenes list from the workspace and if it exists, to only ingest data since the last scene.");
		onlyScenesSinceLastRun.setRequired(false);
		allOptions.addOption(onlyScenesSinceLastRun);

		final Option useCachedScenes = new Option(
				USE_CACHED_SCENES_OPTION,
				false,
				"An option to run against the existing scenes catalog in the workspace directory if it exists.");
		useCachedScenes.setRequired(false);
		allOptions.addOption(useCachedScenes);
	}

	public String getWorkspaceDir() {
		return workspaceDir;
	}

	public String getCqlExpression() {
		return cqlFilter;
	}

	public Filter getCqlFilter() {
		if (cqlFilter != null) {
			try {
				return ECQL.toFilter(cqlFilter);
			}
			catch (final CQLException e) {
				LOGGER.error(
						"Unable to parse CQL expession",
						e);
				System.exit(-1);
			}
		}
		return null;
	}

	public boolean isUseCachedScenes() {
		return useCachedScenes;
	}

	public boolean isOnlyScenesSinceLastRun() {
		return onlyScenesSinceLastRun;
	}

	public int getNBestScenes() {
		return nBestScenes;
	}
}
