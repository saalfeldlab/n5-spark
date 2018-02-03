package org.janelia.saalfeldlab.n5.spark;

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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

public class N5ScalePyramidDownsamplerSpark
{
	private static final int MAX_PARTITIONS = 15000;
	private static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";

	/**
	 * Generates a scale pyramid for a given dataset. Each scale level is downsampled by the given downsampling factors.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * Reuses the block size of the given dataset.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	public static int[][] downsample(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath,
			final int[] downsamplingFactors ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = fullScaleAttributes.getDimensions();
		final int[] blockSize = fullScaleAttributes.getBlockSize();
		final int dim = dimensions.length;

		final List< int[] > scales = new ArrayList<>();
		{
			final int[] fullScaleDownsamplingFactors = new int[ dim ];
			Arrays.fill( fullScaleDownsamplingFactors, 1 );
			scales.add( fullScaleDownsamplingFactors );
		}

		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );

		// loop over scale levels
		for ( int scale = 1; ; ++scale )
		{
			final int[] downsampledBlockSize = blockSize;

			final int[] scaleFactors = new int[ dim ];
			for ( int d = 0; d < dim; ++d )
				scaleFactors[ d ] = ( int ) Math.round( Math.pow( downsamplingFactors[ d ], scale ) );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				downsampledDimensions[ d ] = dimensions[ d ] / scaleFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
				break;

			final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( rootOutputPath, "s" + ( scale - 1 ) ).toString();
			final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();

			N5DownsamplerSpark.downsample(
					sparkContext,
					n5Supplier,
					inputDatasetPath,
					outputDatasetPath,
					downsamplingFactors
				);

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleFactors );
			scales.add( scaleFactors);
		}

		return scales.toArray( new int[ 0 ][] );
	}

	/**
	 * <p>
	 * Generates a scale pyramid for a given dataset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * </p><p>
	 * Assumes that the pixel resolution is the same in X and Y.
	 * Each scale level is downsampled by 2 in XY, and by the corresponding factors in Z to be as close as possible to isotropic.
	 * Reuses the block size of the given dataset, and adjusts the block sizes in Z to be consistent with the scaling factors.
	 * </p>
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 * @param voxelDimensions
	 * 			Pixel resolution of the data
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	/*public static int[][] downsampleIsotropic(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath,
			final VoxelDimensions voxelDimensions ) throws IOException
	{
		final double[] pixelResolutionRatio = ( voxelDimensions != null ? IsotropicScalingEstimator.getPixelResolutionRatio( voxelDimensions ) : null );

		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( datasetPath );
		final long[] fullScaleDimensions = fullScaleAttributes.getDimensions();
		final int[] fullScaleCellSize = fullScaleAttributes.getBlockSize();

		final int dim = fullScaleDimensions.length;

		// TODO: add option to pass custom downsampling factors as parameter
		final int downsamplingFactor = 2;

		final List< int[] > scales = new ArrayList<>();
		{
			final int[] fullScaleDownsamplingFactors = new int[ dim ];
			Arrays.fill( fullScaleDownsamplingFactors, 1 );
			scales.add( fullScaleDownsamplingFactors );
		}

		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
//		final String xyGroupPath = Paths.get( rootOutputPath, "xy" ).toString();

		// loop over scale levels
		for ( int scale = 1; ; ++scale )
		{
			final IsotropicScalingParameters isotropicScalingParameters = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactors(
					scale,
					fullScaleCellSize,
					pixelResolutionRatio,
					downsamplingFactor
				);
			final int[] cellSize = isotropicScalingParameters.cellSize;
			final int[] downsamplingFactors = isotropicScalingParameters.downsamplingFactors;

			final long[] downsampledDimensions = fullScaleDimensions.clone();
			for ( int d = 0; d < downsampledDimensions.length; ++d )
				downsampledDimensions[ d ] /= downsamplingFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() <= 1 || Arrays.stream( downsampledDimensions ).max().getAsLong() <= Arrays.stream( cellSize ).max().getAsInt() )
				break;

			if ( Arrays.stream( downsamplingFactors ).min().getAsInt() == Arrays.stream( downsamplingFactors ).max().getAsInt() )
			{
				// downsampling factors are equal, downsample in all dimensions at once
				final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( rootOutputPath, "s" + ( scale - 1 ) ).toString();
				final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
				n5.createDataset(
						outputDatasetPath,
						downsampledDimensions,
						cellSize,
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputDatasetPath, outputDatasetPath );
			}
			else
			{
				// downsampling factors are different, need to downsample using intermediate groups
				final TreeMap< Integer, List< Integer > > dimensionsSortedByDownsampleFactor = new TreeMap<>();
				for ( int d = 0; d < downsamplingFactors.length; ++d )
				{
					if ( !dimensionsSortedByDownsampleFactor.containsKey( downsamplingFactors[ d ] ) )
						dimensionsSortedByDownsampleFactor.put( downsamplingFactors[ d ], new ArrayList<>() );
					dimensionsSortedByDownsampleFactor.get( downsamplingFactors[ d ] ).add( d );
				}

				for ( final Entry< Integer, List< Integer > > dimensionsToDownsample : dimensionsSortedByDownsampleFactor.entrySet() )
				{
					final int[] relativeDownsamplingFactors = new int[ dim ];
					Arrays.fill( relativeDownsamplingFactors, 1 );
					for ( final int dimensionToDownsample : dimensionsToDownsample.getValue() )
						relativeDownsamplingFactors[ dimensionToDownsample ] = dimensionsToDownsample.getKey();

				}
			}







			if ( !needIntermediateDownsamplingInXY )
			{
				// downsample in XYZ
				final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( rootOutputPath, "s" + ( scale - 1 ) ).toString();
				final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
				n5.createDataset(
						outputDatasetPath,
						downsampledDimensions,
						cellSize,
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputDatasetPath, outputDatasetPath );
			}
			else
			{
				// downsample in XY
				final String inputXYDatasetPath = scale == 1 ? datasetPath : Paths.get( xyGroupPath, "s" + ( scale - 1 ) ).toString();
				final String outputXYDatasetPath = Paths.get( xyGroupPath, "s" + scale ).toString();
				n5.createDataset(
						outputXYDatasetPath,
						new long[] { downsampledDimensions[ 0 ], downsampledDimensions[ 1 ], fullScaleDimensions[ 2 ] },
						new int[] { cellSize[ 0 ], cellSize[ 1 ], fullScaleCellSize[ 2 ] },
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputXYDatasetPath, outputXYDatasetPath );

				// downsample in Z
				final String inputDatasetPath = outputXYDatasetPath;
				final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
				n5.createDataset(
						outputDatasetPath,
						downsampledDimensions,
						cellSize,
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputDatasetPath, outputDatasetPath );
			}

			final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, downsamplingFactors );
			scales.add( downsamplingFactors );
		}

		if ( needIntermediateDownsamplingInXY )
			N5RemoveSpark.remove( sparkContext, n5Supplier, xyGroupPath );

		return scales.toArray( new int[ 0 ][] );
	}*/

	/**
	 * <p>
	 * Generates a scale pyramid for a given dataset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * </p><p>
	 * Assumes that the pixel resolution is the same in X and Y.
	 * Each scale level is downsampled by 2 in XY, and by the corresponding factors in Z to be as close as possible to isotropic.
	 * Reuses the block size of the given dataset, and adjusts the block sizes in Z to be consistent with the scaling factors.
	 * </p>
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 * @param voxelDimensions
	 * 			Pixel resolution of the data
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	/*public static int[][] downsampleIsotropicOld(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath,
			final VoxelDimensions voxelDimensions ) throws IOException
	{
		final double pixelResolutionZtoXY = ( voxelDimensions != null ? IsotropicScalingEstimator.getPixelResolutionZtoXY( voxelDimensions ) : 1 );
		final boolean needIntermediateDownsamplingInXY = ( pixelResolutionZtoXY != 1 );

		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( datasetPath );

		final long[] fullScaleDimensions = fullScaleAttributes.getDimensions();
		final int[] fullScaleCellSize = fullScaleAttributes.getBlockSize();

		final List< int[] > scales = new ArrayList<>();
		scales.add( new int[] { 1, 1, 1 } );

		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		final String xyGroupPath = Paths.get( rootOutputPath, "xy" ).toString();

		// loop over scale levels
		for ( int scale = 1; ; ++scale )
		{
			final IsotropicScalingParameters isotropicScalingParameters = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( scale, fullScaleCellSize, pixelResolutionZtoXY );
			final int[] cellSize = isotropicScalingParameters.cellSize;
			final int[] downsamplingFactors = isotropicScalingParameters.downsamplingFactors;

			final long[] downsampledDimensions = fullScaleDimensions.clone();
			for ( int d = 0; d < downsampledDimensions.length; ++d )
				downsampledDimensions[ d ] /= downsamplingFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() <= 1 || Arrays.stream( downsampledDimensions ).max().getAsLong() <= Arrays.stream( cellSize ).max().getAsInt() )
				break;

			if ( !needIntermediateDownsamplingInXY )
			{
				// downsample in XYZ
				final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( rootOutputPath, "s" + ( scale - 1 ) ).toString();
				final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
				n5.createDataset(
						outputDatasetPath,
						downsampledDimensions,
						cellSize,
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputDatasetPath, outputDatasetPath );
			}
			else
			{
				// downsample in XY
				final String inputXYDatasetPath = scale == 1 ? datasetPath : Paths.get( xyGroupPath, "s" + ( scale - 1 ) ).toString();
				final String outputXYDatasetPath = Paths.get( xyGroupPath, "s" + scale ).toString();
				n5.createDataset(
						outputXYDatasetPath,
						new long[] { downsampledDimensions[ 0 ], downsampledDimensions[ 1 ], fullScaleDimensions[ 2 ] },
						new int[] { cellSize[ 0 ], cellSize[ 1 ], fullScaleCellSize[ 2 ] },
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputXYDatasetPath, outputXYDatasetPath );

				// downsample in Z
				final String inputDatasetPath = outputXYDatasetPath;
				final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
				n5.createDataset(
						outputDatasetPath,
						downsampledDimensions,
						cellSize,
						fullScaleAttributes.getDataType(),
						fullScaleAttributes.getCompression()
					);
				downsampleImpl( sparkContext, n5Supplier, inputDatasetPath, outputDatasetPath );
			}

			final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, downsamplingFactors );
			scales.add( downsamplingFactors );
		}

		if ( needIntermediateDownsamplingInXY )
			N5RemoveSpark.remove( sparkContext, n5Supplier, xyGroupPath );

		return scales.toArray( new int[ 0 ][] );
	}*/


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );

		final int[][] scales;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5DownsamplingSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
//			if ( parsedArgs.getPixelResolution() == null )
//				scales = downsample( sparkContext, n5Supplier, parsedArgs.getInputDatasetPath() );
//			else
//				scales = downsampleIsotropic( sparkContext, n5Supplier, parsedArgs.getInputDatasetPath(), new FinalVoxelDimensions( "", parsedArgs.getPixelResolution() ) );

			scales = downsample( sparkContext, n5Supplier, parsedArgs.getInputDatasetPath(), parsedArgs.getDownsamplingFactors() );
		}

		System.out.println();
		System.out.println( "Scale levels:" );
		for ( int s = 0; s < scales.length; ++s )
			System.out.println( "  " + s + ": " + Arrays.toString( scales[ s ] ) );
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

//		@Option(name = "-r", aliases = { "--pixelResolution" }, required = false,
//				usage = "Pixel resolution of the data. Used to determine downsampling factors in Z to make the scale levels as close to isotropic as possible.")
//		private String pixelResolution;

		@Option(name = "-f", aliases = { "--factors" }, required = false,
				usage = "Downsampling factors.")
		private String downsamplingFactors;

//		@Option(name = "--offset", required = false,
//				usage = "Offset.")
//		private String offset;

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
//		public double[] getPixelResolution() { return CmdUtils.parseDoubleArray( pixelResolution ); }
		public int[] getDownsamplingFactors() { return CmdUtils.parseIntArray( downsamplingFactors ); }
//		public long[] getOffset() { return CmdUtils.parseLongArray( offset ); }
	}
}
