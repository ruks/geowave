package mil.nga.giat.geowave.service.healthimpl;

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
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureImpl;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeospatialExtent {

	private static final Logger logger = LogManager
			.getLogger(GeospatialExtent.class);
	private final static String instanceName = "geowave";
	private final static String zooServers = "127.0.0.1";
	private final static String password = "password";
	private final static String user = "root";
	private final static String namespace = "ruks";

	private final static String table = "ruks_SPATIAL_VECTOR_IDX";

	public static void main(String[] args) throws Exception {

		getTabletPolygon(table);
	}

	public static void getTabletPolygon(String table) {

		Instance accInstance = new ZooKeeperInstance(instanceName, zooServers);
		AuthenticationToken authToken = new PasswordToken(password);
		Connector connector;
		try {
			connector = accInstance.getConnector(user, authToken);
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			return;
		}

		List<Range> splits = getSplits(connector, accInstance);
		System.out.println("splits " + splits.size());
		for (Range range : splits) {
			extent(table, range, connector);
		}
	}

	private static List<Range> getSplits(Connector connector,
			Instance accInstance) {
		TableOperations op = connector.tableOperations();
		System.out.println(op.tableIdMap());

		String tableid = op.tableIdMap().get(table);
		ArrayList<Range> ranges = new ArrayList<Range>();

		try {
			List<Text> list = new ArrayList<Text>(op.listSplits(table));
			System.out.println(list.size());

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			ClientContext ctx = new ClientContext(accInstance, new Credentials(
					user, new PasswordToken(password)), clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tableid));

			TabletLocation tt;
			// String loc;

			Range r;
			String uuid;
			KeyExtent ke;

			for (int i = 0; i < list.size(); i++) {
				tt = tl.locateTablet(ctx, list.get(i), false, false);
				System.out.println(tt.tablet_location);
				ke = tt.tablet_extent;
				// loc = tl.locateTablet(ctx, ke.getEndRow(), false,
				// false).tablet_location;
				r = new Range(list.get(i), ke.getEndRow());
				ranges.add(ke.toDataRange());
				uuid = ke.getUUID().toString();
				System.out.println(uuid);
			}

			Text first, last;
			Key[] kk = read(list.get(list.size() - 1), table, connector);
			first = kk[0].getRow();
			last = kk[1].getRow();

			tt = tl.locateTablet(ctx, first, false, false);
			System.out.println(tt.tablet_location);
			ke = tt.tablet_extent;
			// loc = tl.locateTablet(ctx, last, false, false).tablet_location;
			r = new Range(first, last);
			ranges.add(r);
			System.out.println(ke.getUUID());

		} catch (AccumuloException | TableNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage(), e1);
			e1.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			e.printStackTrace();
		}

		return ranges;
	}

	private static Key[] read(Text end, String table, Connector connector) {

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = connector.createScanner(table, auths);
			scan.setRange(new Range(end, null));
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			int j = 0;
			Key first = null, last;
			Key k = null;
			for (Entry<Key, Value> entry : scan) {
				k = entry.getKey();
				if (j++ == 1) {
					first = k;
				}
			}
			last = k;
			return new Key[] { first, last };

		} catch (TableNotFoundException e) {
			e.printStackTrace();
			logger.error(e.getMessage(), e);
			e.printStackTrace();
			return null;
		}

	}

	private static void extent(String table, Range range, Connector connector) {

		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = connector.createScanner(table, auths);
			scan.setRange(range);
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			ArrayList<SimpleFeatureImpl> list = new ArrayList<SimpleFeatureImpl>();
			for (Entry<Key, Value> entry : scan) {
				AccumuloRowId id = new AccumuloRowId(entry.getKey());
				ByteArrayId bid = new ByteArrayId(id.getAdapterId());

				AccumuloOperations ao = new BasicAccumuloOperations(connector,
						namespace);
				AccumuloAdapterStore a = new AccumuloAdapterStore(ao);
				DataAdapter<?> adapter = a.getAdapter(bid);

				Object o = AccumuloUtils.decodeRow(entry.getKey(),
						entry.getValue(), adapter, index);

				SimpleFeatureImpl pa = (SimpleFeatureImpl) o;
				list.add(pa);

			}

			System.out.println(list.size());

			Coordinate[] points = new Coordinate[list.size()];
			for (int i = 0; i < list.size(); i++) {
				SimpleFeatureImpl si = list.get(i);
				double x = (double) si.getAttribute("Latitude");
				double y = (double) si.getAttribute("Longitude");
				points[i] = new Coordinate(x, y);
			}

			ConvexHull c = new ConvexHull(points, new GeometryFactory());
			Geometry geometry = c.getConvexHull();
			System.out.println(geometry.getNumPoints());
			System.out.println(geometry);

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			e.printStackTrace();
		}

	}

}
