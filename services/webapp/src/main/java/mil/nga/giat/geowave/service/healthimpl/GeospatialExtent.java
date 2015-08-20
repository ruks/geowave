package mil.nga.giat.geowave.service.healthimpl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.service.jaxbbean.RangeBean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureImpl;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeospatialExtent
{

	private static final Logger logger = LogManager.getLogger(GeospatialExtent.class);
	private String instanceName;
	private String zooServers;
	private String password;
	private String user;
	private String namespace;
	private Connector connector;
	private Instance accInstance;

	/**
	 * 
	 * @param instanceName
	 *            Geowave instance name
	 * @param zooServers
	 *            Zooserver Host name
	 * @param user
	 *            username for Geowave instance
	 * @param password
	 *            password for Geowave instance
	 */
	public GeospatialExtent(
			String instanceName,
			String zooServers,
			String user,
			String password ) {
		super();
		this.instanceName = instanceName;
		this.zooServers = zooServers;
		this.password = password;
		this.user = user;

		// create zookeeper instance
		this.accInstance = new ZooKeeperInstance(
				this.instanceName,
				this.zooServers);
		AuthenticationToken authToken = new PasswordToken(
				this.password);

		// Getting connector from instance
		try {
			connector = accInstance.getConnector(
					this.user,
					authToken);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			// logger.error(
			// e.getMessage(),
			// e);
			System.out.println(e.getMessage());
		}
	}

	// private final static String table = "ruks_SPATIAL_VECTOR_IDX";

	/**
	 * Testing main method
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(
			String[] args )
			throws Exception {

		String instanceName = GeowavePropertyReader.readProperty(GeowaveConstant.instanceName);
		String zooServers = GeowavePropertyReader.readProperty(GeowaveConstant.zooServers);
		String user = GeowavePropertyReader.readProperty(GeowaveConstant.user);
		String pass = GeowavePropertyReader.readProperty(GeowaveConstant.pass);

		GeospatialExtent ex = new GeospatialExtent(
				instanceName,
				zooServers,
				user,
				pass);
		String table = "namespace_SPATIAL_VECTOR_IDX";
		List<RangeBean> splits = ex.getSplits(table);
		System.out.println("splits " + splits.size());
		for (RangeBean bean : splits) {
			Coordinate[] points = ex.extent(
					table,
					bean.getRange());
			Geometry g = ex.getConvexHull(points);
			System.out.println(g);
		}
	}

	/**
	 * Getting ranges for table's tablets
	 * 
	 * @param table
	 *            Tablename
	 * @return list of tablet information including ranges
	 */
	public List<RangeBean> getSplits(
			String table ) {
		// taking the table operation form the connector
		TableOperations op = this.connector.tableOperations();

		// getting tablet id from table name
		String tableid = op.tableIdMap().get(
				table);
		ArrayList<RangeBean> ranges = new ArrayList<RangeBean>();

		try {
			// get the split points of the table
			List<Text> list = new ArrayList<Text>(
					op.listSplits(table));

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			ClientContext ctx = new ClientContext(
					this.accInstance,
					new Credentials(
							user,
							new PasswordToken(
									password)),
					clientConf);
			TabletLocator tl = TabletLocator.getLocator(
					ctx,
					new Text(
							tableid));

			TabletLocation tt;

			RangeBean bean;
			String obscuredExtent;
			KeyExtent ke;
			MessageDigest digester;
			String uuid;
			try {
				digester = MessageDigest.getInstance("MD5");
			}
			catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}

			// iterate through the split points
			for (int i = 0; i < list.size(); i++) {

				// getting tabletLocation class
				tt = tl.locateTablet(
						ctx,
						list.get(i),
						false,
						false);

				// getting extent of each tablet
				ke = tt.tablet_extent;

				if (ke.getEndRow() != null && ke.getEndRow().getLength() > 0) {
					digester.update(
							ke.getEndRow().getBytes(),
							0,
							ke.getEndRow().getLength());
				}
				// getting tablet name
				obscuredExtent = Base64.encodeBase64String(digester.digest());
				uuid = ke.getUUID().toString(); // getting tablet uuid

				// create RangeBean object to hold tablet information
				bean = new RangeBean(
						ke.toDataRange(),
						obscuredExtent,
						uuid);
				ranges.add(bean);
				//
			}

			// calculate seperatly for last tablet
			Text first, last;

			Key[] kk;
			if (list.isEmpty()) {// if table contain one tablet

				// get the first and last rows
				kk = this.read(
						null,
						null,
						table);
			}
			else {

				// get the first and last rows of last tablet
				kk = this.read(
						list.get(list.size() - 1),
						null,
						table);
			}
			first = kk[0].getRow();
			last = kk[1].getRow();

			// getting tablet location
			tt = tl.locateTablet(
					ctx,
					first,
					false,
					false);

			// get tablet range and create Range class
			ke = tt.tablet_extent;
			Range r = new Range(
					first,
					last);

			// getting name
			obscuredExtent = Base64.encodeBase64String(digester.digest());
			uuid = ke.getUUID().toString(); // getting tablet uuid
			bean = new RangeBean(
					r,
					obscuredExtent,
					uuid);
			ranges.add(bean); // adding tablet info to list

		}
		catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			// logger.error(
			// e1.getMessage(),
			// e1);
			// e1.printStackTrace();
			System.out.println(e1.getMessage());
		}
		catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			// logger.error(
			// e.getMessage(),
			// e);
			// e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return ranges; // return tablet ranges
	}

	/**
	 * find the first and last rows of each table using table split points
	 * 
	 * @param start
	 *            starting split point
	 * @param end
	 *            ending split point
	 * @param table
	 *            table name
	 * @return return first and last tablet row
	 */
	private Key[] read(
			Text start,
			Text end,
			String table ) {

		try {

			// create Scanner to read rows
			Authorizations auths = new Authorizations();
			Scanner scan = this.connector.createScanner(
					table,
					auths);

			// set the Range to the Scanner
			scan.setRange(new Range(
					start,
					end));

			// setting iterator to read data as a row
			IteratorSetting itSettings = new IteratorSetting(
					1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			int j = 0;
			Key first = null, last;
			Key k = null;

			// iterate through the list of rows
			for (Entry<Key, Value> entry : scan) {
				k = entry.getKey();
				if (j++ == 1) {
					first = k; // take second row as next tablet starting point
				}
			}
			last = k; // last row as last point
			return new Key[] {
				first,
				last
			};

		}
		catch (TableNotFoundException e) {
			// e.printStackTrace();
			logger.error(
					e.getMessage(),
					e);
			// e.printStackTrace();
			return null;
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}

	}

	/**
	 * Find the extent of a tablet using it's range
	 * 
	 * @param table
	 *            name of the table
	 * @param range
	 *            range of the table
	 * @return list of coordinate representing extent
	 */
	public Coordinate[] extent(
			String table,
			Range range ) {

		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		try {

			Authorizations auths = new Authorizations();
			// creating scanner to read
			Scanner scan = this.connector.createScanner(
					table,
					auths);

			// set the scanner range to table range
			scan.setRange(range);
			IteratorSetting itSettings = new IteratorSetting(
					1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			// list to hold table features
			ArrayList<SimpleFeatureImpl> list = new ArrayList<SimpleFeatureImpl>();

			// iterate through each rows to find feature
			for (Entry<Key, Value> entry : scan) {
				AccumuloRowId id = new AccumuloRowId(
						entry.getKey());// getting AccumuloRowId using key
				ByteArrayId bid = new ByteArrayId(
						id.getAdapterId()); // getting bytearray id

				namespace = NamespaceOperation.getNamespaceOfTable(table);
				AccumuloOperations ao = new BasicAccumuloOperations(
						this.connector,
						namespace);
				AccumuloAdapterStore a = new AccumuloAdapterStore(
						ao);
				DataAdapter<?> adapter = a.getAdapter(bid);

				// getting features for the table
				Object o = AccumuloUtils.decodeRow(
						entry.getKey(),
						entry.getValue(),
						adapter,
						index);

				SimpleFeatureImpl pa = (SimpleFeatureImpl) o;
				list.add(pa); // add to list

			}

			Coordinate[] points = new Coordinate[list.size()];
			// extract coordinates from feature
			for (int i = 0; i < list.size(); i++) {
				SimpleFeatureImpl si = list.get(i);
				double x = (double) si.getAttribute("Latitude");
				double y = (double) si.getAttribute("Longitude");
				points[i] = new Coordinate(
						x,
						y);
			}

			return points;

		}
		catch (TableNotFoundException | IndexOutOfBoundsException e) { // TODO
																		// Auto-generated
																		// catch
																		// block
			// logger.error(
			// e.getMessage(),
			// e);
			System.out.println(e.getMessage());
			// e.printStackTrace();
			return null;
		}

	}

	/**
	 * Find ConvexHull of the extent
	 * 
	 * @param points
	 *            list of points representing tablet extent
	 * @return list of points representing convexHull
	 */
	public Geometry getConvexHull(
			Coordinate[] points ) {
		ConvexHull c = new ConvexHull(
				points,
				new GeometryFactory());// creating convexHull object
		try {
			Geometry geometry = c.getConvexHull(); // get the geometry
													// representing convexHull
			return geometry;
		}
		catch (Exception e) {
			return null;
		}

	}

}
