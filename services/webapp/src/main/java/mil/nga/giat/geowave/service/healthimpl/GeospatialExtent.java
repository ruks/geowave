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
	private String instanceName = "geowave";
	private String zooServers = "127.0.0.1";
	private String password = "password";
	private String user = "root";
	private String namespace = "ruks";
	private Connector connector;
	private Instance accInstance;

	public GeospatialExtent(String instanceName, String zooServers,
			String password, String user, String namespace) {
		super();
		this.instanceName = instanceName;
		this.zooServers = zooServers;
		this.password = password;
		this.user = user;
		this.namespace = namespace;

		this.accInstance = new ZooKeeperInstance(this.instanceName,
				this.zooServers);
		AuthenticationToken authToken = new PasswordToken(password);

		try {
			connector = accInstance.getConnector(user, authToken);
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
		}
	}

	// private final static String table = "ruks_SPATIAL_VECTOR_IDX";

	public static void main(String[] args) throws Exception {

		GeospatialExtent ex = new GeospatialExtent("geowave", "127.0.0.1",
				"password", "root", "test4");
		String table = "test4_SPATIAL_VECTOR_IDX";
		List<Range> splits = ex.getSplits(table);
		System.out.println("splits " + splits.size());
		for (Range range : splits) {
			Coordinate[] points = ex.extent(table, range);
			Geometry g = ex.getConvexHull(points);
			System.out.println(g);
		}
	}

	public List<Range> getSplits(String table) {
		TableOperations op = this.connector.tableOperations();

		String tableid = op.tableIdMap().get(table);
		ArrayList<Range> ranges = new ArrayList<Range>();

		try {
			List<Text> list = new ArrayList<Text>(op.listSplits(table));

			ClientConfiguration clientConf = ClientConfiguration.loadDefault();

			ClientContext ctx = new ClientContext(this.accInstance,
					new Credentials(user, new PasswordToken(password)),
					clientConf);
			TabletLocator tl = TabletLocator.getLocator(ctx, new Text(tableid));

			TabletLocation tt;

			Range r;
//			String uuid;
			KeyExtent ke;

			for (int i = 0; i < list.size(); i++) {
				tt = tl.locateTablet(ctx, list.get(i), false, false);
				ke = tt.tablet_extent;
				ranges.add(ke.toDataRange());
//				uuid = ke.getUUID().toString();
			}

			Text first, last;
			Key[] kk = this.read(list.get(list.size() - 1), table);
			first = kk[0].getRow();
			last = kk[1].getRow();

			tt = tl.locateTablet(ctx, first, false, false);
			ke = tt.tablet_extent;
			r = new Range(first, last);
			ranges.add(r);

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

	private Key[] read(Text end, String table) {

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = this.connector.createScanner(table, auths);
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

	public Coordinate[] extent(String table, Range range) {

		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		try {

			Authorizations auths = new Authorizations();
			Scanner scan = this.connector.createScanner(table, auths);
			scan.setRange(range);
			IteratorSetting itSettings = new IteratorSetting(1,
					WholeRowIterator.class);
			scan.addScanIterator(itSettings);

			ArrayList<SimpleFeatureImpl> list = new ArrayList<SimpleFeatureImpl>();
			for (Entry<Key, Value> entry : scan) {
				AccumuloRowId id = new AccumuloRowId(entry.getKey());
				ByteArrayId bid = new ByteArrayId(id.getAdapterId());

				AccumuloOperations ao = new BasicAccumuloOperations(
						this.connector, namespace);
				AccumuloAdapterStore a = new AccumuloAdapterStore(ao);
				DataAdapter<?> adapter = a.getAdapter(bid);

				Object o = AccumuloUtils.decodeRow(entry.getKey(),
						entry.getValue(), adapter, index);

				SimpleFeatureImpl pa = (SimpleFeatureImpl) o;
				list.add(pa);

			}

			Coordinate[] points = new Coordinate[list.size()];
			for (int i = 0; i < list.size(); i++) {
				SimpleFeatureImpl si = list.get(i);
				double x = (double) si.getAttribute("Latitude");
				double y = (double) si.getAttribute("Longitude");
				points[i] = new Coordinate(x, y);
			}

			return points;

		} catch (TableNotFoundException e) { // TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			e.printStackTrace();
			return null;
		}

	}

	public Geometry getConvexHull(Coordinate[] points) {
		ConvexHull c = new ConvexHull(points, new GeometryFactory());
		Geometry geometry = c.getConvexHull();
		return geometry;
	}

}
