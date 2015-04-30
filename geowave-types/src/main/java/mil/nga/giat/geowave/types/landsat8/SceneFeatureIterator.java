package mil.nga.giat.geowave.types.landsat8;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
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
import com.google.common.io.LineReader;
import com.vividsolutions.jts.geom.Polygon;

public class SceneFeatureIterator implements
		FeatureIterator<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SceneFeatureIterator.class);
	private static final String SCENES_GZ_URL = "http://landsat-pds.s3.amazonaws.com/scene_list.gz";
	private static SimpleDateFormat AQUISITION_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.SSS");
	private final String SCENES_DIR = "scenes";
	private final String COMPRESSED_FILE_NAME = "scene_list.gz";
	private final String CSV_FILE_NAME = "scene_list";
	private final String TEMP_CSV_FILE_NAME = "scene_list.tmp";
	private CSVParser parser;
	private Iterator<SimpleFeature> iterator;

	public SceneFeatureIterator(
			final boolean onlyScenesSinceLastRun,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException {
		init(
				new File(
						workspaceDir,
						SCENES_DIR),
				onlyScenesSinceLastRun,
				new WRS2GeometryStore(
						workspaceDir),
				cqlFilter);
	}

	private void init(
			final File scenesDir,
			final boolean onlyScenesSinceLastRun,
			final WRS2GeometryStore geometryStore,
			final Filter cqlFilter )
			throws IOException {
		scenesDir.mkdirs();
		final File compressedFile = new File(
				scenesDir,
				COMPRESSED_FILE_NAME);
		final File csvFile = new File(
				scenesDir,
				CSV_FILE_NAME);
		final File tempCsvFile = new File(
				scenesDir,
				TEMP_CSV_FILE_NAME);
		if (compressedFile.exists()) {
			compressedFile.delete();
		}
		if (tempCsvFile.exists()) {
			tempCsvFile.delete();
		}
		InputStream in = null;
		// first download the gzipped file
		try {
			in = new URL(
					SCENES_GZ_URL).openStream();
			IOUtils.copyLarge(
					in,
					new FileOutputStream(
							compressedFile));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read scenes from public S3",
					e);
			throw e;
		}
		finally {
			if (in != null) {
				IOUtils.closeQuietly(in);
			}
		}
		// next unzip to CSV
		GzipCompressorInputStream gzIn = null;
		FileOutputStream out = null;
		try {
			final FileInputStream fin = new FileInputStream(
					compressedFile);
			final BufferedInputStream bin = new BufferedInputStream(
					fin);
			out = new FileOutputStream(
					csvFile);
			gzIn = new GzipCompressorInputStream(
					bin);
			final byte[] buffer = new byte[1024];
			int n = 0;
			while (-1 != (n = gzIn.read(buffer))) {
				out.write(
						buffer,
						0,
						n);
			}
			// once we have a csv we can cleanup the compressed file
			compressedFile.delete();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to extract scenes file",
					e);
			throw e;
		}
		finally {
			if (out != null) {
				IOUtils.closeQuietly(out);
			}
			if (gzIn != null) {
				IOUtils.closeQuietly(gzIn);
			}
		}
		long startLine = 0;
		if (onlyScenesSinceLastRun && csvFile.exists()) {
			// seek the number of lines of the existing file
			final LineReader lines = new LineReader(
					new FileReader(
							csvFile));
			while (lines.readLine() != null) {
				startLine++;
			}
		}
		if (csvFile.exists()) {
			csvFile.delete();
		}
		tempCsvFile.renameTo(csvFile);
		parser = new CSVParser(
				new FileReader(
						csvFile),
				CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord());
		final Iterator<CSVRecord> csvIterator = parser.iterator();
		// we skip the header, so only skip to start line 1
		while ((startLine > 1) && csvIterator.hasNext()) {
			startLine--;
			csvIterator.next();
		}

		// initialize the feature type
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.add(
				"shape",
				Polygon.class);
		typeBuilder.add(
				"acquisitionDate",
				Date.class);
		typeBuilder.add(
				"cloudCover",
				Double.class);
		typeBuilder.add(
				"processingLevel",
				String.class);
		typeBuilder.add(
				"path",
				Integer.class);
		typeBuilder.add(
				"row",
				Integer.class);
		final SimpleFeatureType type = typeBuilder.buildFeatureType();
		// wrap the iterator with a feature conversion and a filter (if
		// provided)

		iterator = Iterators.transform(
				csvIterator,
				new CSVToFeatureTransform(
						geometryStore,
						type));
		if (cqlFilter != null) {
			iterator = Iterators.filter(
					iterator,
					new CqlFilterPredicate(
							cqlFilter));
		}
	}

	@Override
	public void close() {
		if (parser != null) {
			try {
				parser.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close CSV parser",
						parser);
			}
		}
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

	private static class CSVToFeatureTransform implements
			Function<CSVRecord, SimpleFeature>
	{
		// shape (Geometry), entityId (String), acquisitionDate (Date),
		// cloudCover (double), processingLevel (String), path (int), row (int)
		private final WRS2GeometryStore wrs2Geometry;
		private final SimpleFeatureBuilder featureBuilder;

		public CSVToFeatureTransform(
				final WRS2GeometryStore wrs2Geometry,
				final SimpleFeatureType type ) {
			this.wrs2Geometry = wrs2Geometry;

			featureBuilder = new SimpleFeatureBuilder(
					type);
		}

		// entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url
		@Override
		public SimpleFeature apply(
				final CSVRecord input ) {
			final String entityId = input.get("entityId");
			final double cloudCover = Double.parseDouble(input.get("cloudCover"));
			final String processingLevel = input.get("processingLevel");
			final int path = Integer.parseInt(input.get("path"));
			final int row = Integer.parseInt(input.get("row"));

			final Polygon shape = wrs2Geometry.getGeometry(
					path,
					row);
			featureBuilder.add(shape);
			Date aquisitionDate;
			try {
				aquisitionDate = AQUISITION_DATE_FORMAT.parse(input.get("acquisitionDate"));
				featureBuilder.add(aquisitionDate);
			}
			catch (final ParseException e) {
				LOGGER.warn(
						"Unable to parse aquisition date",
						e);

				featureBuilder.add(null);
			}

			featureBuilder.add(cloudCover);
			featureBuilder.add(processingLevel);
			featureBuilder.add(path);
			featureBuilder.add(row);
			return featureBuilder.buildFeature(entityId);
		}
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
