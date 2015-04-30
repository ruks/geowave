package mil.nga.giat.geowave.types.landsat8;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.NoSuchElementException;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SceneFeatureIterator implements
		FeatureIterator<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SceneFeatureIterator.class);
	private static final String SCENES_GZ_URL = "http://landsat-pds.s3.amazonaws.com/scene_list.gz";

	private final String SCENES_DIR = "scenes";
	private final String COMPRESSED_FILE_NAME = "scene_list.gz";
	private final String CSV_FILE_NAME = "scene_list";
	private final String TEMP_CSV_FILE_NAME = "scene_list.tmp";
	private final boolean onlyScenesSinceLastRun;
	private final Filter cqlFilter;
	private final WRS2GeometryStore geometryStore;

	public SceneFeatureIterator(
			final boolean onlyScenesSinceLastRun,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException {
		this.onlyScenesSinceLastRun = onlyScenesSinceLastRun;
		this.cqlFilter = cqlFilter;
		init(new File(
				workspaceDir,
				SCENES_DIR));
		geometryStore = new WRS2GeometryStore(
				workspaceDir);
	}

	private void init(
			final File scenesDir )
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
			//once we have a csv we can cleanup the compressed file
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
		final long startLine;
		if (onlyScenesSinceLastRun) {
			// seek the number of lines of the existing file
		}
	}

	@Override
	public void close() {

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		// TODO Auto-generated method stub
		return null;
	}

}
