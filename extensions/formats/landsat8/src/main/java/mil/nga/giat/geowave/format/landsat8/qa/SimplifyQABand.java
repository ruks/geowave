package mil.nga.giat.geowave.format.landsat8.qa;

import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;

public class SimplifyQABand
{
	public DataBuffer translate(
			DataBuffer input ) {
		return input;
	}
	
	public SampleModel translate(SampleModel model){
		return model;
	}
}
