package mil.nga.giat.geowave.test.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class AttributesSubsetQueryIT
{
	private static final Logger LOGGER = Logger.getLogger(AttributesSubsetQueryIT.class);

	private static File tempAccumuloDir;
	private static MiniAccumuloCluster accumulo;
	private static SimpleFeatureType simpleFeatureType;
	private static FeatureDataAdapter dataAdapter;
	private static DataStore dataStore;

	private static final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private static final String ACCUMULO_PASSWORD = "Ge0wave";

	// constants for attributes of SimpleFeatureType
	private static final String CITY_ATTRIBUTE = "city";
	private static final String STATE_ATTRIBUTE = "state";
	private static final String POPULATION_ATTRIBUTE = "population";
	private static final String LAND_AREA_ATTRIBUTE = "landArea";
	private static final String GEOMETRY_ATTRIBUTE = "geometry";

	private static final Collection<String> allAttributes = Arrays.asList(
			CITY_ATTRIBUTE,
			STATE_ATTRIBUTE,
			POPULATION_ATTRIBUTE,
			LAND_AREA_ATTRIBUTE,
			GEOMETRY_ATTRIBUTE);

	// points used to construct bounding box for queries
	private static final Coordinate guadalajara = new Coordinate(
			-103.3500,
			20.6667);
	private static final Coordinate atlanta = new Coordinate(
			-84.3900,
			33.7550);

	@BeforeClass
	public static void setup()
			throws IOException,
			InterruptedException,
			AccumuloException,
			AccumuloSecurityException,
			SchemaException {

		simpleFeatureType = getSimpleFeatureType();

		dataAdapter = new FeatureDataAdapter(
				simpleFeatureType);

		tempAccumuloDir = Files.createTempDir();

		accumulo = new MiniAccumuloCluster(
				new MiniAccumuloConfig(
						tempAccumuloDir,
						ACCUMULO_PASSWORD));

		accumulo.start();

		final String ACCUMULO_USER = "root";
		final String TABLE_NAMESPACE = "";

		dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumulo.getZooKeepers(),
						accumulo.getInstanceName(),
						ACCUMULO_USER,
						ACCUMULO_PASSWORD,
						TABLE_NAMESPACE));

		ingestSampleData();
	}

	@Test
	public void testResultsContainAllAttributes()
			throws IOException {

		final CloseableIterator<SimpleFeature> matches = dataStore.query(
				index,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								guadalajara,
								atlanta))));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for each SimpleFeature attribute
		verifyResults(
				matches,
				3,
				allAttributes);
	}

	@Test
	public void testResultsContainCityOnly()
			throws IOException {

		final List<String> attributesSubset = Arrays.asList(CITY_ATTRIBUTE);

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				index,
				attributesSubset,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								guadalajara,
								atlanta))));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for a subset of attributes (city) and nulls for the
		// rest
		verifyResults(
				results,
				3,
				attributesSubset);
	}

	@Test
	public void testResultsContainCityAndPopulation()
			throws IOException {

		final List<String> attributesSubset = Arrays.asList(
				CITY_ATTRIBUTE,
				POPULATION_ATTRIBUTE);

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				index,
				attributesSubset,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								guadalajara,
								atlanta))));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for a subset of attributes (city, population) and
		// nulls for the rest
		verifyResults(
				results,
				3,
				attributesSubset);
	}

	private void verifyResults(
			final CloseableIterator<SimpleFeature> results,
			final int numExpectedResults,
			final Collection<String> attributesExpected )
			throws IOException {

		int numResults = 0;
		SimpleFeature currentFeature;
		Object currentAttributeValue;

		while (results.hasNext()) {

			currentFeature = results.next();
			numResults++;

			for (String currentAttribute : allAttributes) {

				currentAttributeValue = currentFeature.getAttribute(currentAttribute);

				if (attributesExpected.contains(currentAttribute) || currentAttribute.equals(GEOMETRY_ATTRIBUTE)) {
					// geometry will always be included since indexed by spatial
					// dimensionality
					Assert.assertNotNull(
							"Expected non-null " + currentAttribute + " value!",
							currentAttributeValue);
				}
				else {
					Assert.assertNull(
							"Expected null " + currentAttribute + " value!",
							currentAttributeValue);
				}
			}
		}

		results.close();

		Assert.assertEquals(
				"Unexpected number of query results",
				numExpectedResults,
				numResults);
	}

	@AfterClass
	public static void cleanup()
			throws IOException,
			InterruptedException {

		try {
			accumulo.stop();
		}
		finally {
			FileUtils.deleteDirectory(tempAccumuloDir);
		}
	}

	private static SimpleFeatureType getSimpleFeatureType()
			throws SchemaException {

		return DataUtilities.createType(
				"testCityData",
				CITY_ATTRIBUTE + ":String," + STATE_ATTRIBUTE + ":String," + POPULATION_ATTRIBUTE + ":Double," + LAND_AREA_ATTRIBUTE + ":Double," + GEOMETRY_ATTRIBUTE + ":Geometry");
	}

	private static void ingestSampleData() {

		LOGGER.info("Ingesting canned data...");

		dataStore.ingest(
				dataAdapter,
				index,
				buildCityDataSet().iterator());

		LOGGER.info("Ingest complete.");
	}

	private static List<SimpleFeature> buildCityDataSet() {

		final List<SimpleFeature> points = new ArrayList<>();

		// http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
		points.add(buildSimpleFeature(
				"New York",
				"New York",
				8405837,
				302.6,
				new Coordinate(
						-73.9385,
						40.6643)));
		points.add(buildSimpleFeature(
				"Los Angeles",
				"California",
				3884307,
				468.7,
				new Coordinate(
						-118.4108,
						34.0194)));
		points.add(buildSimpleFeature(
				"Chicago",
				"Illinois",
				2718782,
				227.6,
				new Coordinate(
						-87.6818,
						41.8376)));
		points.add(buildSimpleFeature(
				"Houston",
				"Texas",
				2195914,
				599.6,
				new Coordinate(
						-95.3863,
						29.7805)));
		points.add(buildSimpleFeature(
				"Philadelphia",
				"Pennsylvania",
				1553165,
				134.1,
				new Coordinate(
						-75.1333,
						40.0094)));
		points.add(buildSimpleFeature(
				"Phoenix",
				"Arizona",
				1513367,
				516.7,
				new Coordinate(
						-112.088,
						33.5722)));
		points.add(buildSimpleFeature(
				"San Antonio",
				"Texas",
				1409019,
				460.9,
				new Coordinate(
						-98.5251,
						29.4724)));
		points.add(buildSimpleFeature(
				"San Diego",
				"California",
				1355896,
				325.2,
				new Coordinate(
						-117.135,
						32.8153)));
		points.add(buildSimpleFeature(
				"Dallas",
				"Texas",
				1257676,
				340.5,
				new Coordinate(
						-96.7967,
						32.7757)));
		points.add(buildSimpleFeature(
				"San Jose",
				"California",
				998537,
				176.5,
				new Coordinate(
						-121.8193,
						37.2969)));

		return points;
	}

	private static SimpleFeature buildSimpleFeature(
			String city,
			String state,
			double population,
			double landArea,
			Coordinate coordinate ) {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				simpleFeatureType);

		builder.set(
				CITY_ATTRIBUTE,
				city);
		builder.set(
				STATE_ATTRIBUTE,
				state);
		builder.set(
				POPULATION_ATTRIBUTE,
				population);
		builder.set(
				LAND_AREA_ATTRIBUTE,
				landArea);
		builder.set(
				GEOMETRY_ATTRIBUTE,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
