package mil.nga.giat.geowave.format.landsat8;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.store.FeatureIteratorIterator;
import org.geotools.data.store.ReTypingFeatureIterator;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class BandFeatureIterator implements
		FeatureIterator<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BandFeatureIterator.class);
	private final static NumberFormat pathRowFormatter = NumberFormat.getIntegerInstance();
	static {
		pathRowFormatter.setMaximumIntegerDigits(3);
		pathRowFormatter.setMinimumIntegerDigits(3);
	}
	private static final String DOWNLOAD_PREFIX = "http://landsat-pds.s3.amazonaws.com/L8";
	protected static final String BANDS_TYPE_NAME = "band";
	protected static final String BAND_ATTRIBUTE_NAME = "band";
	protected static final String SIZE_ATTRIBUTE_NAME = "sizeMB";
	private Iterator<SimpleFeature> iterator;
	private final SceneFeatureIterator sceneIterator;

	public BandFeatureIterator(
			final boolean onlyScenesSinceLastRun,
			final boolean useCachedScenes,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException {
		this(
				new SceneFeatureIterator(
						onlyScenesSinceLastRun,
						useCachedScenes,
						cqlFilter,
						workspaceDir),
				cqlFilter);
	}

	public BandFeatureIterator(
			final SceneFeatureIterator sceneIterator,
			final Filter cqlFilter ) {
		this.sceneIterator = sceneIterator;
		init(cqlFilter);
	}

	private void init(
			final Filter cqlFilter ) {
		final SimpleFeatureType sceneType = sceneIterator.getFeatureType();
		// initialize the feature type
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.init(sceneType);
		typeBuilder.setName(BANDS_TYPE_NAME);
		typeBuilder.add(
				BAND_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				SIZE_ATTRIBUTE_NAME,
				Float.class);
		final SimpleFeatureType bandType = typeBuilder.buildFeatureType();
		// wrap the iterator with a feature conversion and a filter (if
		// provided)

		iterator = Iterators.concat(Iterators.transform(
				new FeatureIteratorIterator<SimpleFeature>(
						new ReTypingFeatureIterator(
								sceneIterator,
								sceneType,
								bandType)),
				new SceneToBandFeatureTransform(
						bandType)));
		if (cqlFilter != null) {
			final String[] attributes = DataUtilities.attributeNames(
					cqlFilter,
					bandType);
			// we can rely on the scene filtering if we don't have to check any
			// specific band filters
			if (ArrayUtils.contains(
					attributes,
					BAND_ATTRIBUTE_NAME) || ArrayUtils.contains(
					attributes,
					SIZE_ATTRIBUTE_NAME)) {
				// disable filtering in the scene iterator if the attributes
				// used are only in the bands
				sceneIterator.setFilterEnabled(false);
				// and rely on the band filter
				iterator = Iterators.filter(
						iterator,
						new CqlFilterPredicate(
								cqlFilter));
			}
		}
	}

	@Override
	public void close() {
		sceneIterator.close();
	}

	@Override
	public boolean hasNext() {
		if (iterator != null) {
			return iterator.hasNext();
		}
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		if (iterator != null) {
			return iterator.next();
		}
		return null;
	}

	private static class SceneToBandFeatureTransform implements
			Function<SimpleFeature, Iterator<SimpleFeature>>
	{
		private final SimpleFeatureBuilder featureBuilder;

		public SceneToBandFeatureTransform(
				final SimpleFeatureType type ) {
			featureBuilder = new SimpleFeatureBuilder(
					type);
		}

		@Override
		public Iterator<SimpleFeature> apply(
				final SimpleFeature scene ) {
			final String entityId = scene.getID();
			final int path = (int) scene.getAttribute("path");
			final int row = (int) scene.getAttribute("row");
			final List<SimpleFeature> bands = new ArrayList<SimpleFeature>();
			final String indexHtml = getDownloadIndexHtml(
					entityId,
					path,
					row);
			List<String> htmlLines;
			try {
				htmlLines = IOUtils.readLines(new URL(
						indexHtml).openStream());

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
						double divisor = 1;
						if (endIndex < 0) {
							endIndex = line.indexOf("KB)");
							divisor = 1000;
						}
						if (endIndex < 0) {
							continue;
						}
						// rather than match on a specific string for the
						// beginning of the number, let's be flexible and
						// match on several preceding characters and then
						// strip out non-numerics
						beginIndex = endIndex - 6;

						String sizeStr = line.substring(
								beginIndex,
								endIndex);
						sizeStr = sizeStr.replaceAll(
								"[^\\d.]",
								"");
						final double mb = Double.parseDouble(sizeStr) / divisor;
						featureBuilder.init(scene);
						featureBuilder.set(
								SIZE_ATTRIBUTE_NAME,
								mb);
						featureBuilder.set(
								BAND_ATTRIBUTE_NAME,
								bandId);
						bands.add(featureBuilder.buildFeature(entityId + "_" + bandId));
					}
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read " + indexHtml,
						e);
			}
			return bands.iterator();
		}
	}

	protected static String getDownloadPath(
			final String entityId,
			final int path,
			final int row ) {
		return DOWNLOAD_PREFIX + "/" + pathRowFormatter.format(path) + "/" + pathRowFormatter.format(row) + "/" + entityId;
	}

	protected static String getDownloadIndexHtml(
			final String entityId,
			final int path,
			final int row ) {
		return getDownloadPath(
				entityId,
				path,
				row) + "/index.html";
	}

	private static class CqlFilterPredicate implements
			Predicate<SimpleFeature>
	{
		private final Filter cqlFilter;

		public CqlFilterPredicate(
				final Filter cqlFilter ) {
			this.cqlFilter = cqlFilter;
		}

		@Override
		public boolean apply(
				final SimpleFeature input ) {
			return cqlFilter.evaluate(input);
		}

	}
}
