/*-
 * #%L
 * N5 Spark
 * %%
 * Copyright (C) 2017 - 2020 Igor Pisarev, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
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
