package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

	public Landsat8LocalIngestCLIDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		super.nextBand(
				band,
				analysisInfo);
		if (ingestOptions.isCoveragePerBand()) {
			// ingest this band
			// convert the simplefeature into a map
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
				final RasterDataAdapter adapter = new RasterDataAdapter(
						coverageName,
						new HashMap<String, String>(),
						coverage,
						tileSize,
						ingestOptions.isCreatePyramid(),
						ingestOptions.isCreateHistogram(),
						new NoDataMergeStrategy());
			}
			catch (IOException | TemplateException e) {
				LOGGER.error(
						"Unable to ingest band " + band.getID() + " because coverage name cannot be resolved from template",
						e);
			}
		}
		else {
			lastSceneBands.add(band);
		}
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
		if (!ingestOptions.isCoveragePerBand()) {
			if (!lastSceneBands.isEmpty()) {
				// ingest as single image for all bands
			}
			lastSceneBands.clear();
		}
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
