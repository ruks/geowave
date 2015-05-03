package mil.nga.giat.geowave.types.landsat8;

import java.io.IOException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import mil.nga.giat.geowave.ingest.CLIOperationDriver;
import mil.nga.giat.geowave.ingest.CommandLineUtils;
import mil.nga.giat.geowave.types.landsat8.WRS2GeometryStore.WRS2Key;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Landsat8AnalyzeCLIDriver implements
		CLIOperationDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8IngestCLIDriver.class);
	private static final String DOWNLOAD_PREFIX = "http://landsat-pds.s3.amazonaws.com/L8";
	private final String operation;
	private final NumberFormat pathRowFormatter;
	private Landsat8CommandLineOptions landsatOptions;

	public Landsat8AnalyzeCLIDriver(
			final String operation ) {
		this.operation = operation;
		pathRowFormatter = NumberFormat.getIntegerInstance();
		pathRowFormatter.setMaximumIntegerDigits(3);
		pathRowFormatter.setMinimumIntegerDigits(3);
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		allOptions.addOption(CommandLineUtils.getHelpOption());
		applyOptionsInternal(allOptions);

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				allOptions,
				args);
		CommandLineUtils.parseHelpOption(
				commandLine,
				allOptions,
				operation);
		parseOptionsInternal(commandLine);

	}

	protected void runInternal(
			final String[] args ) {
		try {
			try (SceneFeatureIterator scenes = new SceneFeatureIterator(
					landsatOptions.isOnlyScenesSinceLastRun(),
					landsatOptions.getCqlFilter(),
					landsatOptions.getWorkspaceDir())) {
				final TreeMap<String, Double> bandIdToMbMap = new TreeMap<String, Double>();
				int sceneCount = 0;
				Set<WRS2Key> wrs2Keys = new HashSet<WRS2Key>();
				int minRow = Integer.MAX_VALUE;
				int minPath = Integer.MAX_VALUE;
				int maxRow = Integer.MIN_VALUE;
				int maxPath = Integer.MIN_VALUE;
				double minLat = Double.MAX_VALUE;
				double minLon = Double.MAX_VALUE;
				double maxLat = -Double.MAX_VALUE;
				double maxLon = -Double.MAX_VALUE;
				while (scenes.hasNext()) {
					final SimpleFeature scene = scenes.next();
					final String entityId = scene.getID();
					final int path = (int) scene.getAttribute("path");
					final int row = (int) scene.getAttribute("row");
					final String indexHtml = getDownloadIndexHtml(
							entityId,
							path,
							row);
					final List<String> htmlLines = IOUtils.readLines(new URL(
							indexHtml).openStream());
					final TreeMap<String, Double> entityBandIdToMbMap = new TreeMap<String, Double>();
					System.out.println("Entity ID: " + entityId);
					for (final String line : htmlLines) {
						// read everything before the tif
						int endIndex = line.indexOf(".TIF\"");
						if (endIndex > 0) {
							// read everything after the underscore
							int beginIndex = line.indexOf("_") + 1;
							final String bandId = line.substring(
									beginIndex,
									endIndex);
							endIndex = line.indexOf("MB)");
							// rather than match on a specific string for the
							// beginning of the number, let's be flexible and
							// match on several preceding characters and then
							// strip out non-numerics
							beginIndex = endIndex - 6;

							String mbStr = line.substring(
									beginIndex,
									endIndex);
							mbStr = mbStr.replaceAll(
									"[^\\d.]",
									"");
							final double mb = Double.parseDouble(mbStr);
							entityBandIdToMbMap.put(
									bandId,
									mb);
							wrs2Keys.add(new WRS2Key(
									path,
									row));
						}
					}

					System.out.println("\n<--   " + entityId + "   -->");
					for (final Entry<String, Double> entry : entityBandIdToMbMap.entrySet()) {
						final String bandId = entry.getKey();
						final double mb = entry.getValue();
						System.out.println("Band " + bandId + ": " + mb);
						Double totalMb = bandIdToMbMap.get(bandId);
						if (totalMb == null) {
							totalMb = 0.0;
						}
						totalMb += mb;
						bandIdToMbMap.put(
								bandId,
								totalMb);
					}
					sceneCount++;
				}

				System.out.println("\n<--   Totals   -->");
				System.out.println("Total Scenes: " + scenes);
				System.out.println("WRS2 Path/Rows covered: " + wrs2Keys.size());
				System.out.println("Row Range: " + wrs2Keys.size());
				System.out.println("Path Range: " + wrs2Keys.size());
				System.out.println("Latitude Range: " + wrs2Keys.size());
				System.out.println("Longitude Range: " + wrs2Keys.size());
				System.out.println("Longitude Range: " + wrs2Keys.size());
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"",
					e);
		}
	}

	protected String getDownloadPath(
			final String entityId,
			final int path,
			final int row ) {
		return DOWNLOAD_PREFIX + "/" + pathRowFormatter.format(path) + "/" + pathRowFormatter.format(row) + "/" + entityId;
	}

	protected String getDownloadIndexHtml(
			final String entityId,
			final int path,
			final int row ) {
		return getDownloadPath(
				entityId,
				path,
				row) + "/index.html";
	}

	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		landsatOptions = Landsat8CommandLineOptions.parseOptions(commandLine);
	}

	protected void applyOptionsInternal(
			final Options allOptions ) {
		Landsat8CommandLineOptions.applyOptions(allOptions);
	}

}
