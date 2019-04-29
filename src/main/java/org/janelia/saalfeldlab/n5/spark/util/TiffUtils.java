package org.janelia.saalfeldlab.n5.spark.util;

import java.util.Arrays;

import ij.IJ;
import ij.ImagePlus;
import loci.plugins.LociExporter;

public class TiffUtils
{
	public static enum TiffCompression
	{
		NONE,
		LZW
	}

	public static void saveAsTiff( final ImagePlus imp, final String outputPath, final TiffCompression compression )
	{
		workaroundImagePlusNSlices( imp );
		switch ( compression )
		{
		case NONE:
			IJ.saveAsTiff( imp, outputPath );
			break;
		case LZW:
			final LociExporter lociExporter = new LociExporter();
			lociExporter.setup( String.format( "outfile=[%s] compression=[LZW] windowless=[TRUE]", outputPath ), imp );
			lociExporter.run( null );
		default:
			break;
		}
	}

	public static ImagePlus openTiff( final String filepath )
	{
		final ImagePlus imp = IJ.openImage( filepath );
		if ( imp != null )
			workaroundImagePlusNSlices( imp );
		return imp;
	}

	private static void workaroundImagePlusNSlices( final ImagePlus imp )
	{
		final int[] possible3rdDim = new int[] { imp.getNChannels(), imp.getNSlices(), imp.getNFrames() };
		Arrays.sort( possible3rdDim );
		if ( possible3rdDim[ 0 ] * possible3rdDim[ 1 ] == 1 )
			imp.setDimensions( 1, possible3rdDim[ 2 ], 1 );
	}
}
