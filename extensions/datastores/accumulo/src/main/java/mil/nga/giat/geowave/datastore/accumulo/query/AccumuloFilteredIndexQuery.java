package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.EntryIteratorWrapper;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

public abstract class AccumuloFilteredIndexQuery extends
		AccumuloQuery
{
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(AccumuloFilteredIndexQuery.class);
	protected final ScanCallback<?> scanCallback;
	private Collection<String> attributes = null;

	public AccumuloFilteredIndexQuery(
			final Index index,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		super(
				index,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public AccumuloFilteredIndexQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				authorizations);
		this.scanCallback = scanCallback;
	}

	protected List<QueryFilter> getClientFilters() {
		return clientFilters;
	}

	protected void setClientFilters(
			final List<QueryFilter> clientFilters ) {
		this.clientFilters = clientFilters;
	}

	public Collection<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(
			Collection<String> attributes ) {
		this.attributes = attributes;
	}

	protected abstract void addScanIteratorSettings(
			final ScannerBase scanner );

	public CloseableIterator<?> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		return query(
				accumuloOperations,
				adapterStore,
				limit,
				false);
	}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<?> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit,
			final boolean withKeys ) {
		if (!accumuloOperations.tableExists(StringUtils.stringFromBinary(index.getId().getBytes()))) {
			LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
			return new CloseableIterator.Empty();
		}
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				limit);

		// a subset of attributes is being requested
		if (attributes != null && !attributes.isEmpty()) {

			CloseableIterator<DataAdapter<?>> dataAdapters = adapterStore.getAdapters();
			// store a reference to the data adapter
			DataAdapter<?> adapter = dataAdapters.next();

			try {
				dataAdapters.close();
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to close iterator",
						e);
			}

			// configure scanner to fetch only the attributes specified
			handleAttributeSubset(
					scanner,
					adapter);
		}

		if (scanner == null) {
			LOGGER.error("Could not get scanner instance, getScanner returned null");
			return new CloseableIterator.Empty();
		}
		addScanIteratorSettings(scanner);
		Iterator it = initIterator(
				adapterStore,
				scanner);
		if ((limit != null) && (limit > 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIteratorWrapper(
				new ScannerClosableWrapper(
						scanner),
				it);
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final ScannerBase scanner ) {
		return new EntryIteratorWrapper(
				adapterStore,
				index,
				scanner.iterator(),
				new FilterList<QueryFilter>(
						clientFilters),
				scanCallback);
	}

	private void handleAttributeSubset(
			final ScannerBase scanner,
			final DataAdapter<?> adapter ) {

		final Text colFam = new Text(
				adapter.getAdapterId().getBytes());

		Set<ByteArrayId> uniqueDimensions = new HashSet<>();
		for (final DimensionField<? extends CommonIndexValue> dimension : index.getIndexModel().getDimensions()) {
			uniqueDimensions.add(dimension.getFieldId());
		}

		// dimension fields must be included
		for (ByteArrayId dimension : uniqueDimensions) {
			scanner.fetchColumn(
					colFam,
					new Text(
							dimension.getBytes()));
		}

		// configure scanner to fetch only the specified attributes
		for (String attribute : attributes) {
			scanner.fetchColumn(
					colFam,
					new Text(
							StringUtils.stringToBinary(attribute)));
		}

	}
}
