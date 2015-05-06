package mil.nga.giat.geowave.format.landsat8;

import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

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
