package org.janelia.saalfeldlab.n5.spark.util;

public class CmdUtils
{
	public static double[] parseDoubleArray( final String str )
	{
		if ( str == null )
			return null;

		final String[] tokens = str.split( "," );
		final double[] values = new double[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Double.parseDouble( tokens[ i ] );
		return values;
	}

	public static int[] parseIntArray( final String str )
	{
		if ( str == null )
			return null;

		final String[] tokens = str.split( "," );
		final int[] values = new int[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Integer.parseInt( tokens[ i ] );
		return values;
	}

	public static long[] parseLongArray( final String str )
	{
		if ( str == null )
			return null;

		final String[] tokens = str.split( "," );
		final long[] values = new long[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Long.parseLong( tokens[ i ] );
		return values;
	}
}
