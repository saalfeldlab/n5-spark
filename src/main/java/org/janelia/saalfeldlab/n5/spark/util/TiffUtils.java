package org.janelia.saalfeldlab.n5.spark.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Arrays;

import ij.IJ;
import ij.ImagePlus;
import loci.plugins.LociExporter;

public class TiffUtils
{
	public enum TiffCompression
	{
		NONE,
		LZW
	}

	public interface TiffReader extends Serializable
	{
		ImagePlus openTiff( final String inputPath ) throws IOException;
	}

	// TODO: can be replaced by DataProvider + PathResolver class hierarchy that is currently implemented in stitching-spark
	public interface TiffWriter extends Serializable
	{
		void saveTiff( final ImagePlus imp, final String outputPath, final TiffCompression compression ) throws IOException;

		void createDirs( final String path ) throws IOException;

		String combinePaths( final String basePath, final String other );
	}

	public static class FileTiffReader implements TiffReader
	{
		@Override
		public ImagePlus openTiff( final String inputPath )
		{
			final ImagePlus imp = IJ.openImage( inputPath );
			if ( imp != null )
				workaroundImagePlusNSlices( imp );
			return imp;
		}
	}

	public static class FileTiffWriter implements TiffWriter
	{
		@Override
		public void saveTiff( final ImagePlus imp, final String outputPath, final TiffCompression compression)
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

		@Override
		public void createDirs( final String path )
		{
			Paths.get( path ).toFile().mkdirs();
		}

		@Override
		public String combinePaths( final String basePath, final String other )
		{
			return Paths.get( basePath, other ).toString();
		}
	}

	private static void workaroundImagePlusNSlices( final ImagePlus imp )
	{
		final int[] possible3rdDim = new int[] { imp.getNChannels(), imp.getNSlices(), imp.getNFrames() };
		Arrays.sort( possible3rdDim );
		if ( possible3rdDim[ 0 ] * possible3rdDim[ 1 ] == 1 )
			imp.setDimensions( 1, possible3rdDim[ 2 ], 1 );
	}
}
