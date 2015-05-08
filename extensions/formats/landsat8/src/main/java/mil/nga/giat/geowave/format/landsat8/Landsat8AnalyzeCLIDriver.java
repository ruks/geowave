package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
					landsatOptions.getNBestScenes(),
					landsatOptions.getCqlFilter(),
					landsatOptions.getWorkspaceDir())) {
				final AnalysisInfo info = new AnalysisInfo();
				while (bands.hasNext()) {
					final SimpleFeature band = bands.next();
					final String entityId = (String) band.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);
					if (info.isNextScene(entityId)) {
						info.nextScene(
								entityId,
								band);
					}
					final String bandId = (String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);
					final float mb = (float) band.getAttribute(BandFeatureIterator.SIZE_ATTRIBUTE_NAME);
					info.addBandInfo(
							bandId,
							mb);
				}
				info.printSceneInfo();
				info.printTotals();
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

	private static class AnalysisInfo
	{
		private final TreeMap<String, Float> bandIdToMbMap = new TreeMap<String, Float>();
		private final TreeMap<String, Float> entityBandIdToMbMap = new TreeMap<String, Float>();
		private int sceneCount = 0;
		private final Set<WRS2Key> wrs2Keys = new HashSet<WRS2Key>();
		private int minRow = Integer.MAX_VALUE;
		private int minPath = Integer.MAX_VALUE;
		private int maxRow = Integer.MIN_VALUE;
		private int maxPath = Integer.MIN_VALUE;
		private double minLat = Double.MAX_VALUE;
		private double minLon = Double.MAX_VALUE;
		private double maxLat = -Double.MAX_VALUE;
		private double maxLon = -Double.MAX_VALUE;
		private String prevEntityId = null;
		private String prevDownloadUrl = null;
		private Date prevDate = null;
		private float prevCloudCover = 0f;
		private long startDate = Long.MAX_VALUE;
		private long endDate = 0;
		private float totalCloudCover = 0f;
		private float minCloudCover = Float.MAX_VALUE;
		private float maxCloudCover = -Float.MAX_VALUE;
		private final Map<String, Integer> processingLevelCounts = new HashMap<String, Integer>();

		private boolean isNextScene(
				final String entityId ) {
			return (prevEntityId == null) || !prevEntityId.equals(entityId);
		}

		private void nextScene(
				final String entityId,
				final SimpleFeature currentBand ) {
			printSceneInfo();
			entityBandIdToMbMap.clear();
			prevEntityId = entityId;
			final int path = (int) currentBand.getAttribute(SceneFeatureIterator.PATH_ATTRIBUTE_NAME);
			final int row = (int) currentBand.getAttribute(SceneFeatureIterator.ROW_ATTRIBUTE_NAME);
			final float cloudCover = (float) currentBand.getAttribute(SceneFeatureIterator.CLOUD_COVER_ATTRIBUTE_NAME);
			final String processingLevel = (String) currentBand.getAttribute(SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME);
			final Date date = (Date) currentBand.getAttribute(SceneFeatureIterator.ACQUISITION_DATE_ATTRIBUTE_NAME);
			prevDownloadUrl = (String) currentBand.getAttribute(SceneFeatureIterator.DOWNLOAD_ATTRIBUTE_NAME);
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
			final Envelope env = ((Geometry) currentBand.getDefaultGeometry()).getEnvelopeInternal();
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

			minCloudCover = Math.min(
					minCloudCover,
					cloudCover);
			maxCloudCover = Math.max(
					maxCloudCover,
					cloudCover);
			totalCloudCover += cloudCover;
			prevCloudCover = cloudCover;

			Integer count = processingLevelCounts.get(processingLevel);
			if (count == null) {
				count = 0;
			}
			count++;
			processingLevelCounts.put(
					processingLevel,
					count);

			startDate = Math.min(
					startDate,
					date.getTime());
			endDate = Math.max(
					endDate,
					date.getTime());
			prevDate = date;
			wrs2Keys.add(new WRS2Key(
					path,
					row));

		}

		private void printSceneInfo() {
			if (prevEntityId != null) {
				sceneCount++;
				System.out.println("\n<--   " + prevEntityId + "   -->");
				System.out.println("Acquisition Date: " + SceneFeatureIterator.AQUISITION_DATE_FORMAT.format(prevDate));
				System.out.println("Cloud Cover: " + prevCloudCover);
				System.out.println("Download URL: " + prevDownloadUrl);
				for (final Entry<String, Float> entry : entityBandIdToMbMap.entrySet()) {
					final String bandId = entry.getKey();
					final float mb = entry.getValue();
					System.out.println("Band " + bandId + ": " + mb + " MB");
					Float totalMb = bandIdToMbMap.get(bandId);
					if (totalMb == null) {
						totalMb = 0.0f;
					}
					totalMb += mb;
					bandIdToMbMap.put(
							bandId,
							totalMb);
				}
			}
		}

		private void addBandInfo(
				final String bandId,
				final float mb ) {
			entityBandIdToMbMap.put(
					bandId,
					mb);
		}

		private void printTotals() {
			System.out.println("\n<--   Totals   -->");
			System.out.println("Total Scenes: " + sceneCount);
			if (sceneCount > 0) {
				System.out.println("Date Range: [" + SceneFeatureIterator.AQUISITION_DATE_FORMAT.format(new Date(
						startDate)) + ", " + SceneFeatureIterator.AQUISITION_DATE_FORMAT.format(new Date(
						endDate)) + "]");
				System.out.println("Cloud Cover Range: [" + minCloudCover + ", " + maxCloudCover + "]");
				System.out.println("Average Cloud Cover: " + (totalCloudCover / sceneCount));
				System.out.println("WRS2 Paths/Rows covered: " + wrs2Keys.size());
				System.out.println("Row Range: [" + minRow + ", " + maxRow + "]");
				System.out.println("Path Range: [" + minPath + ", " + maxPath + "]");
				System.out.println("Latitude Range: [" + minLat + ", " + maxLat + "]");
				System.out.println("Longitude Range: [" + minLon + ", " + maxLon + "]");
				final StringBuffer strBuf = new StringBuffer(
						"Processing Levels: ");
				boolean includeSceneCount = false;
				boolean first = true;
				if (processingLevelCounts.size() > 1) {
					includeSceneCount = true;
				}
				for (final Entry<String, Integer> entry : processingLevelCounts.entrySet()) {
					if (!first) {
						strBuf.append(", ");
					}
					else {
						first = false;
					}
					strBuf.append(entry.getKey());
					if (includeSceneCount) {
						strBuf.append(" (" + entry.getValue() + " scenes)");
					}
				}
				for (final Entry<String, Float> entry : bandIdToMbMap.entrySet()) {
					final String bandId = entry.getKey();
					final float mb = Math.round(entry.getValue() * 10) / 10f;
					final String avg;
					if (sceneCount > 1) {
						avg = "(avg. " + (Math.round((entry.getValue() * 10) / sceneCount) / 10f) + " MB)";
					}
					else {
						avg = "";
					}
					System.out.println("Band " + bandId + ": " + mb + " MB " + avg);
				}
			}
		}
	}
}
