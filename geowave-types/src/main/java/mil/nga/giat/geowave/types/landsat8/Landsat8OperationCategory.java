package mil.nga.giat.geowave.types.landsat8;

import mil.nga.giat.geowave.ingest.CustomOperationCategory;

public class Landsat8OperationCategory extends
		CustomOperationCategory
{
	public Landsat8OperationCategory() {
		super(
				"landsat8",
				"Landsat 8",
				"Operations to analyze, download, and ingest Landsat 8 imagery publicly available on AWS");
	}
}
