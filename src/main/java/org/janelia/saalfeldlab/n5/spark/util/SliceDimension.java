package org.janelia.saalfeldlab.n5.spark.util;

public enum SliceDimension
{
	X( 0 ),
	Y( 1 ),
	Z( 2 );

	private final int d;

	private SliceDimension( final int d )
	{
		this.d = d;
	}

	public int asInteger()
	{
		return d;
	}
}
