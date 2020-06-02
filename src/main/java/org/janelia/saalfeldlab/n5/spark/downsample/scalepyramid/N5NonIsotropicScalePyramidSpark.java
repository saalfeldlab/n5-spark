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
package org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.N5RemoveSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.util.Util;

public class N5NonIsotropicScalePyramidSpark
{
	static class NonIsotropicMetadata3D
	{
		public final long[] dimensions;
		public final int[] cellSize;
		public final int[] downsamplingFactors;

		public NonIsotropicMetadata3D(
				final long[] dimensions,
				final int[] cellSize,
				final int[] downsamplingFactors )
		{
			this.dimensions = dimensions;
			this.cellSize = cellSize;
			this.downsamplingFactors = downsamplingFactors;
		}
	}

	static class NonIsotropicScalePyramidMetadata3D
	{
		static enum MainDimension
		{
			XY,
			Z
		}

		private static final double EPSILON = 1e-10;

		public final List< NonIsotropicMetadata3D > scalesMetadata;
		public final MainDimension mainDimension;
		public final boolean isPowerOfTwo;
		private final double pixelResolutionRatio;

		public NonIsotropicScalePyramidMetadata3D(
				final long[] fullScaleDimensions3D,
				final int[] fullScaleCellSize3D,
				final double[] pixelResolution,
				final boolean isPowerOfTwo )
		{
			if ( fullScaleDimensions3D.length != 3 || fullScaleCellSize3D.length != 3 || ( pixelResolution != null && pixelResolution.length != 3 ) )
				throw new IllegalArgumentException( "Expected fullScaleDimensions3D, fullScaleCellSize3D, pixelResolution arrays of length 3" );

			// determine the main dimension which is the one with better resolution
			final double ratioZtoXY = getPixelResolutionRatioZtoXY( pixelResolution );
			if ( ratioZtoXY > 1 )
			{
				mainDimension = MainDimension.XY;
				pixelResolutionRatio = ratioZtoXY;
			}
			else
			{
				mainDimension = MainDimension.Z;
				pixelResolutionRatio = 1. / ratioZtoXY;
			}

			this.isPowerOfTwo = isPowerOfTwo || (
					Util.isApproxEqual( pixelResolutionRatio, Math.round( pixelResolutionRatio ), EPSILON )
					&& ( Math.round( pixelResolutionRatio ) & ( Math.round( pixelResolutionRatio ) - 1 ) ) == 0
				);

			scalesMetadata = new ArrayList<>();
			init( fullScaleDimensions3D, fullScaleCellSize3D );
		}

		private void init( final long[] fullScaleDimensions, final int[] fullScaleCellSize )
		{
			scalesMetadata.add( new NonIsotropicMetadata3D( fullScaleDimensions.clone(), fullScaleCellSize.clone(), new int[] { 1, 1, 1 } ) );
			for ( int scale = 1; ; ++scale )
			{
				final int mainDownsamplingFactor = 1 << scale;
				final double isotropicScaling = mainDownsamplingFactor / pixelResolutionRatio;
				final int dependentDownsamplingFactor = isPowerOfTwo ? getNearestPowerOfTwo( isotropicScaling ) : Math.max( 1, ( int ) Math.round( isotropicScaling ) );
				final int[] downsamplingFactors;
				if ( mainDimension == MainDimension.XY )
					downsamplingFactors = new int[] { mainDownsamplingFactor, mainDownsamplingFactor, dependentDownsamplingFactor };
				else
					downsamplingFactors = new int[] { dependentDownsamplingFactor, dependentDownsamplingFactor, mainDownsamplingFactor };

				final long[] downsampledDimensions = new long[ fullScaleDimensions.length ];
				for ( int d = 0; d < downsampledDimensions.length; ++d )
					downsampledDimensions[ d ] = fullScaleDimensions[ d ] / downsamplingFactors[ d ];
				if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
					break;

				final int mainCellSize, dependentPreviousCellSize;
				if ( mainDimension == MainDimension.XY )
				{
					mainCellSize = Math.max( fullScaleCellSize[ 0 ], fullScaleCellSize[ 1 ] );
					dependentPreviousCellSize = scalesMetadata.get( scale - 1 ).cellSize[ 2 ];
				}
				else
				{
					mainCellSize = fullScaleCellSize[ 2 ];
					dependentPreviousCellSize = Math.max( scalesMetadata.get( scale - 1 ).cellSize[ 0 ], scalesMetadata.get( scale - 1 ).cellSize[ 1 ] );
				}
				final int dependentFullScaleOptimalCellSize = ( int ) Math.round( mainCellSize / pixelResolutionRatio );
				final int dependentOptimalCellSize = ( int ) Math.round( ( long ) dependentFullScaleOptimalCellSize * mainDownsamplingFactor / ( double ) dependentDownsamplingFactor );
				final int dependentMultipleCellSize = ( int ) Math.round( dependentOptimalCellSize / ( double ) dependentPreviousCellSize ) * dependentPreviousCellSize;
				final int dependentMaxCellSize = Math.max( ( int ) Math.round( dependentFullScaleOptimalCellSize * pixelResolutionRatio ), mainCellSize );
				final int dependentAdjustedCellSize = Math.max( dependentPreviousCellSize, Math.min( dependentMultipleCellSize, ( dependentMaxCellSize / dependentPreviousCellSize ) * dependentPreviousCellSize ) );
				final int[] downsampledCellSize;
				if ( mainDimension == MainDimension.XY )
					downsampledCellSize = new int[] { fullScaleCellSize[ 0 ], fullScaleCellSize[ 1 ], dependentAdjustedCellSize };
				else
					downsampledCellSize = new int[] { dependentAdjustedCellSize, dependentAdjustedCellSize, fullScaleCellSize[ 2 ] };

				scalesMetadata.add( new NonIsotropicMetadata3D( downsampledDimensions, downsampledCellSize, downsamplingFactors ) );
			}
		}

		public int getNumScales()
		{
			return scalesMetadata.size();
		}

		public NonIsotropicMetadata3D getScaleMetadata( final int scale )
		{
			return scalesMetadata.get( scale );
		}

		public int getDependentDownsamplingFactor( final int scale )
		{
			final NonIsotropicMetadata3D scaleMetadata = getScaleMetadata( scale );
			if ( mainDimension == MainDimension.XY )
				return scaleMetadata.downsamplingFactors[ 2 ];
			else
				return Math.max( scaleMetadata.downsamplingFactors[ 0 ], scaleMetadata.downsamplingFactors[ 1 ] );
		}

		public int[] getIntermediateDownsamplingFactors( final int scale )
		{
			final NonIsotropicMetadata3D scaleMetadata = getScaleMetadata( scale ), previousScaleMetadata = getScaleMetadata( scale - 1 );
			if ( mainDimension == MainDimension.XY )
				return new int[] { scaleMetadata.downsamplingFactors[ 0 ] / previousScaleMetadata.downsamplingFactors[ 0 ], scaleMetadata.downsamplingFactors[ 1 ] / previousScaleMetadata.downsamplingFactors[ 1 ], 1 };
			else
				return new int[] { 1, 1, scaleMetadata.downsamplingFactors[ 2 ] / previousScaleMetadata.downsamplingFactors[ 2 ] };
		}

		public static double getPixelResolutionRatioZtoXY( final double[] pixelResolution )
		{
			if ( pixelResolution == null )
				return 1;
			return pixelResolution[ 2 ] / Math.max( pixelResolution[ 0 ], pixelResolution[ 1 ] );
		}

		private static int getNearestPowerOfTwo( final double value )
		{
			return 1 << Math.max( 0, Math.round( ( Math.log( value ) / Math.log( 2 ) ) ) );
		}
	}

	private static final String PIXEL_RESOLUTION_ATTRIBUTE_KEY = "pixelResolution";

	/**
	 * Generates a scale pyramid for a given dataset. Assumes that the pixel resolution is the same in X and Y.
	 * The scale pyramid is constructed in the following way depending on the pixel resolution of the data:<br>
	 * - if the resolution is better in X/Y than in Z: each scale level is downsampled by 2 in X/Y, and by the corresponding factors in Z to be as close as possible to isotropic<br>
	 * - if the resolution is better in Z than in X/Y: each scale level is downsampled by 2 in Z, and by the corresponding factors in X/Y to be as close as possible to isotropic<br>
	 *<p>
	 * Adjusts the block size to be consistent with the scaling factors. Stores the resulting datasets in the same group as the input dataset.
	 *<p>
	 * Works only with 3D and higher dimensionality data. Only the first three dimensions are used for non-isotropic downsampling, and the rest of the dimensions are written out as is.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param fullScaleDatasetPath
	 * @param pixelResolution
	 * @param isPowerOfTwo
	 * @return N5 paths to downsampled datasets
	 * @throws IOException
	 */
	public static List< String > downsampleNonIsotropicScalePyramid(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String fullScaleDatasetPath,
			final double[] pixelResolution,
			final boolean isPowerOfTwo ) throws IOException
	{
		final String outputGroupPath = ( Paths.get( fullScaleDatasetPath ).getParent() != null ? Paths.get( fullScaleDatasetPath ).getParent().toString() : "" );
		return downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				fullScaleDatasetPath,
				outputGroupPath,
				pixelResolution,
				isPowerOfTwo
			);
	}

	/**
	 * Generates a scale pyramid for a given dataset. Assumes that the pixel resolution is the same in X and Y.
	 * The scale pyramid is constructed in the following way depending on the pixel resolution of the data:<br>
	 * - if the resolution is better in X/Y than in Z: each scale level is downsampled by 2 in X/Y, and by the corresponding factors in Z to be as close as possible to isotropic<br>
	 * - if the resolution is better in Z than in X/Y: each scale level is downsampled by 2 in Z, and by the corresponding factors in X/Y to be as close as possible to isotropic<br>
	 *<p>
	 * Adjusts the block size to be consistent with the scaling factors. Stores the resulting datasets in the given output group.
	 *<p>
	 * Works only with 3D and higher dimensionality data. Only the first three dimensions are used for non-isotropic downsampling, and the rest of the dimensions are written out as is.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param fullScaleDatasetPath
	 * @param outputGroupPath
	 * @param pixelResolution
	 * @param isPowerOfTwo
	 * @return N5 paths to downsampled datasets
	 * @throws IOException
	 */
	public static List< String > downsampleNonIsotropicScalePyramid(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String fullScaleDatasetPath,
			final String outputGroupPath,
			final double[] pixelResolution,
			final boolean isPowerOfTwo ) throws IOException
	{
		if ( pixelResolution.length != 3 )
			throw new IllegalArgumentException( "Expected pixelResolution array of length 3, got " + pixelResolution.length );

		if ( !Util.isApproxEqual( pixelResolution[ 0 ], pixelResolution[ 1 ], 1e-10 ) )
			throw new IllegalArgumentException( "Pixel resolution is different in X/Y" );

		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( fullScaleDatasetPath );

		if ( fullScaleAttributes.getNumDimensions() < 3 )
			throw new IllegalArgumentException( "Works only with 3D and higher dimensionality data, got " + fullScaleAttributes.getNumDimensions() + "D" );

		final long[] fullScaleDimensions = fullScaleAttributes.getDimensions();
		final int[] fullScaleCellSize = fullScaleAttributes.getBlockSize();

		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata3D = new NonIsotropicScalePyramidMetadata3D(
				getDimensions3D( fullScaleDimensions ),
				getBlockSizes3D( fullScaleCellSize ),
				pixelResolution,
				isPowerOfTwo
			);

		// prepare for intermediate downsampling if required
		final String intermediateGroupPath;
		if ( !scalePyramidMetadata3D.isPowerOfTwo )
		{
			System.out.println( "Not a power of two, intermediate downsampling in " + scalePyramidMetadata3D.mainDimension + " is required" );
			intermediateGroupPath = Paths.get( outputGroupPath, "intermediate-downsampling-" + scalePyramidMetadata3D.mainDimension ).toString();
			if ( n5.exists( intermediateGroupPath ) )
				throw new RuntimeException( "Group for intermediate downsampling in " + scalePyramidMetadata3D.mainDimension + " already exists: " + intermediateGroupPath );
			n5.createGroup( intermediateGroupPath );
		}
		else
		{
			System.out.println( "Power of two, skip intermediate downsampling in " + scalePyramidMetadata3D.mainDimension );
			intermediateGroupPath = null;
		}

		// check for existence of output datasets and fail if any of them already exist
		// it is safer to do so because otherwise the user may accidentally overwrite useful data
		for ( int scale = 1; scale < scalePyramidMetadata3D.getNumScales(); ++scale )
		{
			final String outputDatasetPath = Paths.get( outputGroupPath, "s" + scale ).toString();
			if ( n5.datasetExists( outputDatasetPath ) )
				throw new RuntimeException( "Output dataset already exists: " + outputDatasetPath );
		}

		final List< String > downsampledDatasets = new ArrayList<>();
		for ( int scale = 1; scale < scalePyramidMetadata3D.getNumScales(); ++scale )
		{
			final NonIsotropicMetadata3D scaleMetadata3D = scalePyramidMetadata3D.getScaleMetadata( scale );
			final String outputDatasetPath = Paths.get( outputGroupPath, "s" + scale ).toString();

			if ( scalePyramidMetadata3D.isPowerOfTwo || scalePyramidMetadata3D.getDependentDownsamplingFactor( scale ) == 1 )
			{
				final String inputDatasetPath = scale == 1 ? fullScaleDatasetPath : Paths.get( outputGroupPath, "s" + ( scale - 1 ) ).toString();

				// intermediate downsampling is not happening yet at this scale level, or is not required at all
				final NonIsotropicMetadata3D previousScaleMetadata3D = scalePyramidMetadata3D.getScaleMetadata( scale - 1 );
				final int[] relativeDownsamplingFactors3D = new int[ scaleMetadata3D.downsamplingFactors.length ];
				for ( int d = 0; d < relativeDownsamplingFactors3D.length; ++d )
				{
					if ( scaleMetadata3D.downsamplingFactors[ d ] % previousScaleMetadata3D.downsamplingFactors[ d ] != 0 )
						throw new RuntimeException( "something went wrong, expected divisible downsampling factors" );
					relativeDownsamplingFactors3D[ d ] = scaleMetadata3D.downsamplingFactors[ d ] / previousScaleMetadata3D.downsamplingFactors[ d ];
				}

				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						outputDatasetPath,
						appendDownsamplingFactorsForUnchangedDimensions( relativeDownsamplingFactors3D, fullScaleAttributes.getNumDimensions() ),
						appendBlockSizesForUnchangedDimensions( scaleMetadata3D.cellSize, fullScaleCellSize )
					);
			}
			else
			{
				final String inputDatasetPath;
				if ( scalePyramidMetadata3D.getDependentDownsamplingFactor( scale - 1 ) == 1 )
				{
					// this is the first scale level where intermediate downsampling is required
					inputDatasetPath = scale == 1 ? fullScaleDatasetPath : Paths.get( outputGroupPath, "s" + ( scale - 1 ) ).toString();
				}
				else
				{
					// there exists an intermediate downsampled export
					inputDatasetPath = Paths.get( intermediateGroupPath, "s" + ( scale - 1 ) ).toString();
				}

				final String intermediateDatasetPath = Paths.get( intermediateGroupPath, "s" + scale ).toString();
				final int[] intermediateDownsamplingFactors3D = scalePyramidMetadata3D.getIntermediateDownsamplingFactors( scale );

				// downsample and store in the intermediate export group
				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						intermediateDatasetPath,
						appendDownsamplingFactorsForUnchangedDimensions( intermediateDownsamplingFactors3D, fullScaleAttributes.getNumDimensions() ),
						appendBlockSizesForUnchangedDimensions( scaleMetadata3D.cellSize, fullScaleCellSize )
					);

				final int[] relativeDownsamplingFactors3D = new int[ intermediateDownsamplingFactors3D.length ];
				for ( int d = 0; d < relativeDownsamplingFactors3D.length; ++d )
					relativeDownsamplingFactors3D[ d ] = intermediateDownsamplingFactors3D[ d ] == 1 ? scaleMetadata3D.downsamplingFactors[ d ] : 1;

				// downsample and store in the output group
				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						intermediateDatasetPath,
						outputDatasetPath,
						appendDownsamplingFactorsForUnchangedDimensions( relativeDownsamplingFactors3D, fullScaleAttributes.getNumDimensions() ),
						appendBlockSizesForUnchangedDimensions( scaleMetadata3D.cellSize, fullScaleCellSize )
					);
			}

			n5.setAttribute( outputDatasetPath, PIXEL_RESOLUTION_ATTRIBUTE_KEY, pixelResolution );

			downsampledDatasets.add( outputDatasetPath );
		}

		if ( !scalePyramidMetadata3D.isPowerOfTwo )
			N5RemoveSpark.remove( sparkContext, n5Supplier, intermediateGroupPath );

		return downsampledDatasets;
	}

	/**
	 * Returns a new array containing the first 3 entries of the given dimensions.
	 *
	 * @param dimensions
	 * @return XYZ dimensions
	 */
	private static long[] getDimensions3D( final long[] dimensions )
	{
		if ( dimensions.length < 3 )
			throw new IllegalArgumentException( "Expected dimensions array of length at least 3, got " + dimensions.length );

		final long[] dimensions3D = new long[ 3 ];
		System.arraycopy( dimensions, 0, dimensions3D, 0, dimensions3D.length );

		return dimensions3D;
	}

	/**
	 * Returns a new array containing the first 3 entries of the given block sizes.
	 *
	 * @param blockSizes
	 * @return XYZ block sizes
	 */
	private static int[] getBlockSizes3D( final int[] blockSizes )
	{
		if ( blockSizes.length < 3 )
			throw new IllegalArgumentException( "Expected blockSizes array of length at least 3, got " + blockSizes.length );

		final int[] blockSizes3D = new int[ 3 ];
		System.arraycopy( blockSizes, 0, blockSizes3D, 0, blockSizes3D.length );

		return blockSizes3D;
	}

	/**
	 * Fills the downsampling factors for the rest of the dimensions with '1', so that only XYZ dimensions are downsampled, and the rest of the dimensions are written out as is.
	 *
	 * @param downsamplingFactors3D
	 * @param numFullDimensions
	 * @return downsampling factors for all dimensions
	 */
	private static int[] appendDownsamplingFactorsForUnchangedDimensions( final int[] downsamplingFactors3D, final int numFullDimensions )
	{
		if ( downsamplingFactors3D.length != 3 )
			throw new IllegalArgumentException( "Expected downsamplingFactors3D array of length 3, got " + downsamplingFactors3D.length );

		if ( numFullDimensions < 3 )
			throw new IllegalArgumentException( "Expected numFullDimensions to be at least 3, got " + numFullDimensions );

		final int[] fullDownsamplingFactors = new int[ numFullDimensions ];
		Arrays.fill( fullDownsamplingFactors, 1 );
		System.arraycopy( downsamplingFactors3D, 0, fullDownsamplingFactors, 0, downsamplingFactors3D.length );

		return fullDownsamplingFactors;
	}

	/**
	 * Fills the block sizes for the rest of the dimensions with the same as in the input data.
	 *
	 * @param blockSizes3D
	 * @param inputDataBlockSizes
	 * @return block sizes for all dimensions
	 */
	private static int[] appendBlockSizesForUnchangedDimensions( final int[] blockSizes3D, final int[] inputDataBlockSizes )
	{
		if ( blockSizes3D.length != 3 )
			throw new IllegalArgumentException( "Expected blockSizes3D array of length 3, got " + blockSizes3D.length );

		if ( inputDataBlockSizes.length < 3 )
			throw new IllegalArgumentException( "Expected inputDataBlockSizes array of length at least 3, got " + inputDataBlockSizes.length );

		final int[] fullBlockSizes = inputDataBlockSizes.clone();
		System.arraycopy( blockSizes3D, 0, fullBlockSizes, 0, blockSizes3D.length );

		return fullBlockSizes;
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5NonIsotropicScalePyramidSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );

			if ( parsedArgs.getOutputGroupPath() != null )
			{
				downsampleNonIsotropicScalePyramid(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getOutputGroupPath(),
						parsedArgs.getPixelResolution(),
						parsedArgs.getIsPowerOfTwo()
					);
			}
			else
			{
				downsampleNonIsotropicScalePyramid(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getPixelResolution(),
						parsedArgs.getIsPowerOfTwo()
					);
			}
		}
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -1467734459169624759L;

		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--outputGroupPath" }, required = false,
				usage = "Path to a group within the N5 container to store the output datasets (e.g. data/group/scale-pyramid).")
		private String outputGroupPath;

		@Option(name = "-r", aliases = { "--pixelResolution" }, required = true,
				usage = "Pixel resolution of the data in um (microns). Depending on whether the resolution is better in X/Y than in Z or vice versa, the downsampling factors are adjusted to make the scale levels as close to isotropic as possible.")
		private String pixelResolution;

		@Option(name = "-p", aliases = { "--powerOfTwo" }, required = false,
				usage = "Forces to generate a power-of-two scale pyramid that is as close to isotropic as possible.")
		private boolean isPowerOfTwo;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
				System.exit( 1 );
			}
		}

		public String getN5Path() { return n5Path; }
		public String getInputDatasetPath() { return inputDatasetPath; }
		public String getOutputGroupPath() { return outputGroupPath; }
		public double[] getPixelResolution() { return CmdUtils.parseDoubleArray( pixelResolution ); }
		public boolean getIsPowerOfTwo() { return isPowerOfTwo; }
	}
}
