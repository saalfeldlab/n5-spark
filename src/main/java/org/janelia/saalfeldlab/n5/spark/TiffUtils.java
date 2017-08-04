package org.janelia.saalfeldlab.n5.spark;

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
}
