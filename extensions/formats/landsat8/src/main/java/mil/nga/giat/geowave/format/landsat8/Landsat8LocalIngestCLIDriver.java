package mil.nga.giat.geowave.format.landsat8;

import java.awt.image.DataBuffer;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class Landsat8LocalIngestCLIDriver extends
		Landsat8DownloadCLIDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Landsat8LocalIngestCLIDriver.class);

	protected Landsat8IngestCommandLineOptions ingestOptions;
	protected AccumuloCommandLineOptions accumuloOptions;
	List<SimpleFeature> lastSceneBands = new ArrayList<SimpleFeature>();
	private Template coverageNameTemplate;
	private IndexWriter writer;

	public Landsat8LocalIngestCLIDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void runInternal(
			final String[] args ) {
		try {
			final DataStore geowaveDataStore = new AccumuloDataStore(
					accumuloOptions.getAccumuloOperations());
			final Index index = accumuloOptions.getIndex(new Index[] {
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				IndexType.SPATIAL_TEMPORAL_RASTER.createDefaultIndex()
			});
			writer = geowaveDataStore.createIndexWriter(index);

			super.runInternal(args);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to connect to Accumulo",
					e);
		}
		finally {
			if (writer != null) {
				try {
					writer.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close Accumulo writer",
							e);
				}
			}
		}
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		super.nextBand(
				band,
				analysisInfo);
		// if (ingestOptions.isCoveragePerBand()) {
		// ingest this band
		// convert the simplefeature into a map to resolve the coverage name
		// using a user supplied freemarker template
		final Map<String, Object> model = new HashMap<String, Object>();
		final SimpleFeatureType type = band.getFeatureType();
		for (final AttributeDescriptor attr : type.getAttributeDescriptors()) {
			final String attrName = attr.getLocalName();
			final Object attrValue = band.getAttribute(attrName);
			if (attrValue != null) {
				model.put(
						attrName,
						attrValue);
			}
		}
		try {
			final String coverageName = FreeMarkerTemplateUtils.processTemplateIntoString(
					coverageNameTemplate,
					model);
			final File geotiffFile = Landsat8DownloadCLIDriver.getDownloadFile(
					band,
					landsatOptions.getWorkspaceDir());
			final GeoTiffReader reader = new GeoTiffReader(
					geotiffFile);
			final GridCoverage2D coverage = reader.read(null);
			// DataBuffer dataBuffer =
			// coverage.getRenderedImage().getData().getDataBuffer();
			// final PixelInterleavedSampleModel sampleModel =
			// (PixelInterleavedSampleModel)
			// coverage.getRenderedImage().getData().getSampleModel();
			// for (dataBuffer.get)
			final MultiPixelPackedSampleModel newSampleModel = new MultiPixelPackedSampleModel(
					DataBuffer.TYPE_BYTE,
					coverage.getRenderedImage().getWidth(),
					coverage.getRenderedImage().getHeight(),
					1);
			final WritableRaster nextRaster = Raster.createWritableRaster(
					newSampleModel,
					null);
			final RenderedImage image = coverage.getRenderedImage();

			// int singleBitMask = 0x0001;
			final int doubleBitMask = 0x0003;
			// int tripleBitMask = 0x0007;
			// long totalFill = 0;
			// long totalDroppedFrame = 0;
			// long totalTerrainOcclusion = 0;

			// long totalUndeterminedWater = 0;
			// long totalNoWater = 0;
			// long totalMaybeWater = 0;
			// long totalYesWater = 0;

			// long totalUndeterminedVegetation = 0;
			// long totalNoVegetation = 0;
			// long totalYesVegetation = 0;
			// long totalMaybeVegetation = 0;

			// long totalUndeterminedSnowIce = 0;
			// long totalNoSnowIce = 0;
			// long totalMaybeSnowIce = 0;
			// long totalYesSnowIce = 0;

			// long none = 0;
			//
			// long totalUndeterminedCirrus = 0;
			// long totalNoCirrus = 0;
			// long totalMaybeCirrus = 0;
			// long totalYesCirrus = 0;
			//
			// long totalUndeterminedClouds = 0;
			// long totalNoClouds = 0;
			// long totalMaybeClouds = 0;
			// long totalYesClouds = 0;
			final Raster data = image.getData();
			for (int x = 0; x < data.getWidth(); x++) {
				for (int y = 0; y < data.getHeight(); y++) {
					final int sample = data.getSample(
							x,
							y,
							0);
					// int r =0;
					// int g = 0;
					// int b =0;
					// if ((sample & tripleBitMask) > 0) {
					// sample =0;
					// totalFill++;
					// }else if ((sample >> 14 & doubleBitMask) == 3 || (sample
					// >> 12 & doubleBitMask) == 3) {
					// sample = 1;
					// r=255;
					// totalYesClouds++;
					// }else if ((sample >> 10 & doubleBitMask) > 1) {
					// // sample = 64;
					// g = 255;
					// totalYesSnowIce++;
					// }
					// else if ((sample >> 4 & doubleBitMask) > 1) {
					// sample = 127;
					// b = 255;
					// totalYesWater++;
					// }
					// else {sample = 255;
					// r =255;
					// g =255;
					// b=255;
					// none++;
					// }
					nextRaster.setSample(
							x,
							y,
							0,
							(((sample >> 10) & doubleBitMask) > 1) ? 1 : 0);
				}
			}
			final GridCoverage2D nextCov = new GridCoverageFactory().create(
					coverageName,
					nextRaster,
					coverage.getEnvelope());
			// BufferedImage writ = new BufferedImage(img.getColorModel(),
			// nextRaster, img.getColorModel().isAlphaPremultiplied(), null);
			// ImageIO.write(nextCov.getRenderedImage(), "jpg", new
			// File("C:\\test\\test.jpg"));
			// long total = data.getHeight()*data.getWidth();
			// System.err.println("total fill: " + (double)totalFill/total);
			// System.err.println("total yes water: " +
			// (double)totalYesWater/total);
			// System.err.println("total yes SnowIce: " +
			// (double)totalYesSnowIce/total);
			// System.err.println("total yes Clouds: " +
			// (double)totalYesClouds/total);
			// System.err.println("total none: " + (double)none/total);

			if (coverage != null) {
				final Map<String, String> metadata = new HashMap<String, String>();
				final String[] mdNames = reader.getMetadataNames(coverage.getName().toString());
				if ((mdNames != null) && (mdNames.length > 0)) {
					for (final String mdName : mdNames) {
						metadata.put(
								mdName,
								reader.getMetadataValue(
										coverageName,
										mdName));
					}
				}
				final RasterDataAdapter adapter = new RasterDataAdapter(
						coverageName,
						metadata,
						nextCov,
						ingestOptions.getTileSize(),
						ingestOptions.isCreatePyramid(),
						ingestOptions.isCreateHistogram(),
						new NoDataMergeStrategy());
				writer.write(
						adapter,
						nextCov);
			}
			else {
				LOGGER.error("Unable to ingest band " + band.getID() + "; cannot read geotiff '" + geotiffFile.getAbsolutePath() + "'");
			}
		}
		catch (IOException | TemplateException e) {
			LOGGER.error(
					"Unable to ingest band " + band.getID() + " because coverage name cannot be resolved from template",
					e);
		}
		// }
		// else {
		// lastSceneBands.add(band);
		// }
	}

	@Override
	protected void lastSceneComplete(
			final AnalysisInfo analysisInfo ) {
		super.lastSceneComplete(analysisInfo);
		processLastScene();
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		processLastScene();
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	protected void processLastScene() {
		// if (!ingestOptions.isCoveragePerBand()) {
		// if (!lastSceneBands.isEmpty()) {
		// TODO ingest as single image for all bands
		// do we need to capture all possible permutations of bands (ie.
		// this scene may not contain all bands for this coverage)?

		// also we need to consider supersampling if the panchromatic
		// band (B8) is provided, because it will be twice the
		// resolution of the other bands

		// }
		// lastSceneBands.clear();
		// }
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		super.parseOptionsInternal(commandLine);
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		ingestOptions = Landsat8IngestCommandLineOptions.parseOptions(commandLine);
		try {
			coverageNameTemplate = new Template(
					"user defined template",
					new StringReader(
							ingestOptions.getCoverageName()),
					new Configuration());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to read freemarker template for coverage name",
					e);
			throw new ParseException(
					e.getMessage());
		}
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		super.applyOptionsInternal(allOptions);
		AccumuloCommandLineOptions.applyOptions(allOptions);
		Landsat8IngestCommandLineOptions.applyOptions(allOptions);
	}
}
