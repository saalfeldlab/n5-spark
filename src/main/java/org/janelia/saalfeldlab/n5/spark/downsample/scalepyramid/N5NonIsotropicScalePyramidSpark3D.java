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
import org.janelia.saalfeldlab.n5.spark.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.N5RemoveSpark;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.util.Util;

public class N5NonIsotropicScalePyramidSpark3D
{
	static class NonIsotropicMetadata
	{
		public final long[] dimensions;
		public final int[] cellSize;
		public final int[] downsamplingFactors;

		public NonIsotropicMetadata(
				final long[] dimensions,
				final int[] cellSize,
				final int[] downsamplingFactors )
		{
			this.dimensions = dimensions;
			this.cellSize = cellSize;
			this.downsamplingFactors = downsamplingFactors;
		}
	}

	static class NonIsotropicScalePyramidMetadata
	{
		public final List< NonIsotropicMetadata > scalesMetadata;

		public NonIsotropicScalePyramidMetadata( final long[] fullScaleDimensions, final int[] fullScaleCellSize, final double[] pixelResolution )
		{
			final double pixelResolutionZtoXY = getPixelResolutionZtoXY( pixelResolution );

			scalesMetadata = new ArrayList<>();
			scalesMetadata.add( new NonIsotropicMetadata( fullScaleDimensions.clone(), fullScaleCellSize.clone(), new int[] { 1, 1, 1 } ) );

			for ( int scale = 1; ; ++scale )
			{
				final int xyDownsamplingFactor = 1 << scale;
				final int isotropicScaling = ( int ) Math.round( xyDownsamplingFactor / pixelResolutionZtoXY );
				final int zDownsamplingFactor = Math.max( isotropicScaling, 1 );
				final int[] downsamplingFactors = new int[] { xyDownsamplingFactor, xyDownsamplingFactor, zDownsamplingFactor };

				final long[] downsampledDimensions = new long[ fullScaleDimensions.length ];
				for ( int d = 0; d < downsampledDimensions.length; ++d )
					downsampledDimensions[ d ] = fullScaleDimensions[ d ] / downsamplingFactors[ d ];

				if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
					break;

				final int fullScaleOptimalCellSize = ( int ) Math.round( Math.max( fullScaleCellSize[ 0 ], fullScaleCellSize[ 1 ] ) / pixelResolutionZtoXY );
				final int zOptimalCellSize = ( int ) Math.round( ( long ) fullScaleOptimalCellSize * xyDownsamplingFactor / ( double ) zDownsamplingFactor );
				final int zPreviousCellSize = scalesMetadata.get( scale - 1 ).cellSize[ 2 ];
				final int zCellSize = ( int ) Math.round( zOptimalCellSize / ( double ) zPreviousCellSize ) * zPreviousCellSize;
				final int zMaxCellSize = Math.max( ( int ) Math.round( fullScaleOptimalCellSize * pixelResolutionZtoXY ), Math.max( fullScaleCellSize[ 0 ], fullScaleCellSize[ 1 ] ) );
				final int zAdjustedCellSize = Math.min( zCellSize, ( zMaxCellSize / zPreviousCellSize ) * zPreviousCellSize );
				final int[] downsampledCellSize = new int[] { fullScaleCellSize[ 0 ], fullScaleCellSize[ 1 ], zAdjustedCellSize };

				scalesMetadata.add( new NonIsotropicMetadata( downsampledDimensions, downsampledCellSize, downsamplingFactors ) );
			}
		}

		public int getNumScales()
		{
			return scalesMetadata.size();
		}

		public NonIsotropicMetadata getScaleMetadata( final int scale )
		{
			return scalesMetadata.get( scale );
		}

		public static double getPixelResolutionZtoXY( final double[] pixelResolution )
		{
			if ( pixelResolution == null )
				return 1;
			return pixelResolution[ 2 ] / Math.max( pixelResolution[ 0 ], pixelResolution[ 1 ] );
		}
	}

	private static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";

	/**
	 * Generates a scale pyramid for a given dataset (3D only). Assumes that the pixel resolution is the same in X and Y.
	 * Each scale level is downsampled by 2 in XY, and by the corresponding factors in Z to be as close as possible to isotropic.
	 * Reuses the block size of the given dataset, and adjusts the block sizes in Z to be consistent with the scaling factors.
	 * Stores the resulting datasets in the same group as the input dataset.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param fullScaleDatasetPath
	 * @param pixelResolution
	 * @return N5 paths to downsampled datasets
	 * @throws IOException
	 */
	public static List< String > downsampleNonIsotropicScalePyramid(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String fullScaleDatasetPath,
			final double[] pixelResolution ) throws IOException
	{
		final String outputGroupPath = ( Paths.get( fullScaleDatasetPath ).getParent() != null ? Paths.get( fullScaleDatasetPath ).getParent().toString() : "" );
		return downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				fullScaleDatasetPath,
				outputGroupPath,
				pixelResolution
			);
	}

	/**
	 * Generates a scale pyramid for a given dataset (3D only). Assumes that the pixel resolution is the same in X and Y.
	 * Each scale level is downsampled by 2 in XY, and by the corresponding factors in Z to be as close as possible to isotropic.
	 * Reuses the block size of the given dataset, and adjusts the block sizes in Z to be consistent with the scaling factors.
	 * Stores the resulting datasets in the given output group.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param fullScaleDatasetPath
	 * @param outputGroupPath
	 * @param pixelResolution
	 * @return N5 paths to downsampled datasets
	 * @throws IOException
	 */
	public static List< String > downsampleNonIsotropicScalePyramid(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String fullScaleDatasetPath,
			final String outputGroupPath,
			final double[] pixelResolution ) throws IOException
	{
		if ( !Util.isApproxEqual( pixelResolution[ 0 ], pixelResolution[ 1 ], 1e-10 ) )
			throw new IllegalArgumentException( "Pixel resolution is different in X/Y" );

		if ( Util.isApproxEqual( NonIsotropicScalePyramidMetadata.getPixelResolutionZtoXY( pixelResolution ), 1.0, 1e-10 ) )
			throw new IllegalArgumentException( "Pixel resolution is the same in X/Y/Z, use regular N5ScalePyramidSpark" );

		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( fullScaleDatasetPath );
		final long[] fullScaleDimensions = fullScaleAttributes.getDimensions();
		final int[] fullScaleCellSize = fullScaleAttributes.getBlockSize();

		// prepare for downsampling in XY
		final String xyGroupPath = Paths.get( outputGroupPath, "intermediate-downsampling-xy" ).toString();
		n5.createGroup( xyGroupPath );

		final List< String > downsampledDatasets = new ArrayList<>();
		final NonIsotropicScalePyramidMetadata scalePyramidMetadata = new NonIsotropicScalePyramidMetadata( fullScaleDimensions, fullScaleCellSize, pixelResolution );

		for ( int scale = 1; scale < scalePyramidMetadata.getNumScales(); ++scale )
		{
			final NonIsotropicMetadata scaleMetadata = scalePyramidMetadata.getScaleMetadata( scale );
			final String outputDatasetPath = Paths.get( outputGroupPath, "s" + scale ).toString();

			if ( scaleMetadata.downsamplingFactors[ 2 ] == 1 )
			{
				final String inputDatasetPath = scale == 1 ? fullScaleDatasetPath : Paths.get( outputGroupPath, "s" + ( scale - 1 ) ).toString();

				// downsampling in Z is not happening yet at this scale level, downsample in XY and save directly in the output group
				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						outputDatasetPath,
						new int[] { 2, 2, 1 },
						scaleMetadata.cellSize
					);
			}
			else
			{
				final String inputDatasetPath;
				if ( scalePyramidMetadata.getScaleMetadata( scale - 1 ).downsamplingFactors[ 2 ] == 1 )
				{
					// this is the first scale level where downsampling in Z is required
					inputDatasetPath = scale == 1 ? fullScaleDatasetPath : Paths.get( outputGroupPath, "s" + ( scale - 1 ) ).toString();
				}
				else
				{
					// there exists an intermediate XY downsampled export
					inputDatasetPath = Paths.get( xyGroupPath, "s" + ( scale - 1 ) ).toString();
				}

				final String xyDatasetPath = Paths.get( xyGroupPath, "s" + scale ).toString();

				// downsample in XY and store in the intermediate export group
				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						xyDatasetPath,
						new int[] { 2, 2, 1 },
						scaleMetadata.cellSize
					);

				// downsample in Z and store in the output group
				N5DownsamplerSpark.downsample(
						sparkContext,
						n5Supplier,
						xyDatasetPath,
						outputDatasetPath,
						new int[] { 1, 1, scaleMetadata.downsamplingFactors[ 2 ] },
						scaleMetadata.cellSize
					);
			}

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleMetadata.downsamplingFactors );
			downsampledDatasets.add( outputDatasetPath );
		}

		N5RemoveSpark.remove( sparkContext, n5Supplier, xyGroupPath );
		return downsampledDatasets;
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5NonIsotropicScalePyramidSpark3D" )
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
						parsedArgs.getPixelResolution()
					);
			}
			else
			{
				downsampleNonIsotropicScalePyramid(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getPixelResolution()
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
				usage = "Pixel resolution of the data. Used to determine downsampling factors in Z to make the scale levels as close to isotropic as possible.")
		private String pixelResolution;

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
	}
}
