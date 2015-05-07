package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineUtils;
import mil.nga.giat.geowave.format.landsat8.WRS2GeometryStore.WRS2Key;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class Landsat8AnalyzeCLIDriver implements
		CLIOperationDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8IngestCLIDriver.class);

	private final String operation;
	private Landsat8CommandLineOptions landsatOptions;

	public Landsat8AnalyzeCLIDriver(
			final String operation ) {
		this.operation = operation;
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
		runInternal(args);
	}

	protected void runInternal(
			final String[] args ) {
		try {
			try (BandFeatureIterator bands = new BandFeatureIterator(
					landsatOptions.isOnlyScenesSinceLastRun(),
					landsatOptions.isUseCachedScenes(),
					landsatOptions.getCqlFilter(),
					landsatOptions.getWorkspaceDir())) {
				final TreeMap<String, Double> bandIdToMbMap = new TreeMap<String, Double>();
				int sceneCount = 0;
				final Set<WRS2Key> wrs2Keys = new HashSet<WRS2Key>();
				int minRow = Integer.MAX_VALUE;
				int minPath = Integer.MAX_VALUE;
				int maxRow = Integer.MIN_VALUE;
				int maxPath = Integer.MIN_VALUE;
				double minLat = Double.MAX_VALUE;
				double minLon = Double.MAX_VALUE;
				double maxLat = -Double.MAX_VALUE;
				double maxLon = -Double.MAX_VALUE;
				String prevEntityId = null;
				final TreeMap<String, Float> entityBandIdToMbMap = new TreeMap<String, Float>();
				while (bands.hasNext()) {
					final SimpleFeature band = bands.next();
					final String entityId = (String) band.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);
					if ((prevEntityId == null) || !prevEntityId.equals(entityId)) {
						System.out.println("\n<--   " + prevEntityId + "   -->");
						for (final Entry<String, Float> entry : entityBandIdToMbMap.entrySet()) {
							final String bandId = entry.getKey();
							final double mb = entry.getValue();
							System.out.println("Band " + bandId + ": " + mb + " MB");
							Double totalMb = bandIdToMbMap.get(bandId);
							if (totalMb == null) {
								totalMb = 0.0;
							}
							totalMb += mb;
							bandIdToMbMap.put(
									bandId,
									totalMb);
						}
						entityBandIdToMbMap.clear();
						prevEntityId = entityId;
						sceneCount++;
						final int path = (int) band.getAttribute(SceneFeatureIterator.PATH_ATTRIBUTE_NAME);
						final int row = (int) band.getAttribute(SceneFeatureIterator.ROW_ATTRIBUTE_NAME);
						minRow = Math.min(
								minRow,
								row);
						maxRow = Math.max(
								maxRow,
								row);
						minPath = Math.min(
								minPath,
								path);
						maxPath = Math.max(
								maxPath,
								path);
						final Envelope env = ((Geometry) band.getDefaultGeometry()).getEnvelopeInternal();
						minLat = Math.min(
								minLat,
								env.getMinY());
						maxLat = Math.max(
								maxLat,
								env.getMaxY());
						minLon = Math.min(
								minLon,
								env.getMinX());
						maxLon = Math.max(
								maxLon,
								env.getMaxX());
						wrs2Keys.add(new WRS2Key(
								path,
								row));
					}
					final String bandId = (String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);
					final float mb = (float) band.getAttribute(BandFeatureIterator.SIZE_ATTRIBUTE_NAME);
					entityBandIdToMbMap.put(
							bandId,
							mb);
				}
				if (prevEntityId != null) {
					System.out.println("\n<--   " + prevEntityId + "   -->");
					for (final Entry<String, Float> entry : entityBandIdToMbMap.entrySet()) {
						final String bandId = entry.getKey();
						final double mb = entry.getValue();
						System.out.println("Band " + bandId + ": " + mb + " MB");
						Double totalMb = bandIdToMbMap.get(bandId);
						if (totalMb == null) {
							totalMb = 0.0;
						}
						totalMb += mb;
						bandIdToMbMap.put(
								bandId,
								totalMb);
					}
				}
				System.out.println("\n<--   Totals   -->");
				System.out.println("Total Scenes: " + sceneCount);
				System.out.println("WRS2 Path/Rows covered: " + wrs2Keys.size());
				System.out.println("Row Range: [" + minRow + ", " + maxRow + "]");
				System.out.println("Path Range: [" + minPath + ", " + maxPath + "]");
				System.out.println("Latitude Range: [" + minLat + ", " + maxLat + "]");
				System.out.println("Longitude Range: [" + minLon + ", " + maxLon + "]");
				for (final Entry<String, Double> entry : bandIdToMbMap.entrySet()) {
					final String bandId = entry.getKey();
					final double mb = entry.getValue();
					System.out.println("Band " + bandId + ": " + mb + " MB (avg. " + (mb / sceneCount) + ")");
					Double totalMb = bandIdToMbMap.get(bandId);
					if (totalMb == null) {
						totalMb = 0.0;
					}
					totalMb += mb;
					bandIdToMbMap.put(
							bandId,
							totalMb);
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"",
					e);
		}
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
