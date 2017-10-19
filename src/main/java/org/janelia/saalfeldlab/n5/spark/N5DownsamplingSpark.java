package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5DownsamplingSpark.IsotropicScalingEstimator.IsotropicScalingParameters;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import bdv.export.Downsample;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;

public class N5DownsamplingSpark
{
	public static class IsotropicScalingEstimator
	{
		public static class IsotropicScalingParameters
		{
			public final int[] cellSize;
			public final int[] downsamplingFactors;

			public IsotropicScalingParameters( final int[] cellSize, final int[] downsamplingFactors )
			{
				this.cellSize = cellSize;
				this.downsamplingFactors = downsamplingFactors;
			}
		}

		public static double getPixelResolutionZtoXY( final VoxelDimensions voxelDimensions )
		{
			return voxelDimensions.dimension( 2 ) / Math.max( voxelDimensions.dimension( 0 ), voxelDimensions.dimension( 1 ) );
		}

		public static IsotropicScalingParameters getOptimalCellSizeAndDownsamplingFactor( final int scaleLevel, final int[] originalCellSize, final double pixelResolutionZtoXY )
		{
			final int xyDownsamplingFactor = 1 << scaleLevel;
			final int isotropicScaling = ( int ) Math.round( xyDownsamplingFactor / pixelResolutionZtoXY );
			final int zDownsamplingFactor = Math.max( isotropicScaling, 1 );
			final int[] downsamplingFactors = new int[] { xyDownsamplingFactor, xyDownsamplingFactor, zDownsamplingFactor };

			final int fullScaleOptimalCellSize = ( int ) Math.round( Math.max( originalCellSize[ 0 ], originalCellSize[ 1 ] ) / pixelResolutionZtoXY );
			final int zOptimalCellSize = ( int ) Math.round( fullScaleOptimalCellSize * xyDownsamplingFactor / ( double ) zDownsamplingFactor );
			// adjust Z cell size to a closest multiple of the original Z cell size
			final int zAdjustedCellSize = ( int ) Math.round( ( zOptimalCellSize / ( double ) fullScaleOptimalCellSize ) ) * fullScaleOptimalCellSize;
			final int[] cellSize = new int[] { originalCellSize[ 0 ], originalCellSize[ 1 ], zAdjustedCellSize };

			return new IsotropicScalingParameters( cellSize, downsamplingFactors );
		}
	}

	private static final int MAX_PARTITIONS = 15000;
	private static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";

	/**
	 * Generates lower scale levels for a given dataset. Each scale level is downsampled by 2 in all dimensions.
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
			final String datasetPath ) throws IOException
	{
		return downsampleIsotropic( sparkContext, n5Supplier, datasetPath, null );
	}

	/**
	 * <p>
	 * Generates lower scale levels for a given dataset.
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
	public static int[][] downsampleIsotropic(
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
						fullScaleAttributes.getCompressionType()
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
						fullScaleAttributes.getCompressionType()
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
						fullScaleAttributes.getCompressionType()
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
	}

	private static < T extends NativeType< T > & RealType< T > > void downsampleImpl(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes inputAttributes = n5.getDatasetAttributes( inputDatasetPath );
		final DatasetAttributes outputAttributes = n5.getDatasetAttributes( outputDatasetPath );

		final long[] inputDimensions = inputAttributes.getDimensions();
		final long[] outputDimensions = outputAttributes.getDimensions();
		final int[] downsamplingFactors = new int[ inputDimensions.length ];
		for ( int d = 0; d < downsamplingFactors.length; ++d )
			downsamplingFactors[ d ] = ( int ) ( inputDimensions[ d ] / outputDimensions[ d ] );

		final int[] outputCellSize = outputAttributes.getBlockSize();
		final int dim = outputCellSize.length;

		final List< Tuple2< Interval, Interval > > sourceAndTargetIntervals = new ArrayList<>();
		final long[] offset = new long[ dim ], sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
		for ( int d = 0; d < dim; )
		{
			for ( int i = 0; i < dim; i++ )
			{
				targetMin[ i ] = offset[ i ];
				targetMax[ i ] = Math.min( targetMin[ i ] + outputCellSize[ i ], outputDimensions[ i ] ) - 1;
				sourceMin[ i ] = targetMin[ i ] * downsamplingFactors[ i ];
				sourceMax[ i ] = targetMax[ i ] * downsamplingFactors[ i ] + ( downsamplingFactors[ i ] - 1 );
			}

			sourceAndTargetIntervals.add( new Tuple2<>( new FinalInterval( sourceMin, sourceMax ), new FinalInterval( targetMin, targetMax ) ) );

			for ( d = 0; d < dim; ++d )
			{
				offset[ d ] += outputCellSize[ d ];
				if ( offset[ d ] < outputDimensions[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}

		sparkContext.parallelize( sourceAndTargetIntervals, Math.min( sourceAndTargetIntervals.size(), MAX_PARTITIONS ) ).foreach( sourceAndTargetInterval ->
		{
			final N5Writer n5Local = n5Supplier.get();

			final RandomAccessibleInterval< T > previousScaleLevelImg = N5Utils.open( n5Local, inputDatasetPath );
			final RandomAccessibleInterval< T > source = Views.offsetInterval( previousScaleLevelImg, sourceAndTargetInterval._1() );
			final Img< T > target = new ArrayImgFactory< T >().create( sourceAndTargetInterval._2(), Util.getTypeFromInterval( source ) );
			Downsample.downsample( source, target, downsamplingFactors );

			final long[] gridPosition = new long[ dim ];
			final CellGrid cellGrid = new CellGrid( outputDimensions, outputCellSize );
			cellGrid.getCellPosition( Intervals.minAsLongArray( sourceAndTargetInterval._2() ), gridPosition );

			N5Utils.saveBlock( target, n5Local, outputDatasetPath, gridPosition );
		} );
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		final int[][] scales;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5DownsamplingSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> N5.openFSWriter( parsedArgs.getN5Path() );
			if ( parsedArgs.getPixelResolution() == null )
				scales = downsample( sparkContext, n5Supplier, parsedArgs.getInputDatasetPath() );
			else
				scales = downsampleIsotropic( sparkContext, n5Supplier, parsedArgs.getInputDatasetPath(), new FinalVoxelDimensions( "", parsedArgs.getPixelResolution() ) );
		}

		System.out.println();
		System.out.println( "Scale levels:" );
		for ( int s = 0; s < scales.length; ++s )
			System.out.println( "  " + s + ": " + Arrays.toString( scales[ s ] ) );
	}

	private static class Arguments
	{
		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-r", aliases = { "--pixelResolution" }, required = false,
				usage = "Pixel resolution of the data. Used to determine downsampling factors in Z to make the scale levels as close to isotropic as possible.")
		private String pixelResolution;

		private boolean parsedSuccessfully = false;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public boolean parsedSuccessfully() { return parsedSuccessfully; }

		public String getN5Path() { return n5Path; }
		public String getInputDatasetPath() { return inputDatasetPath; }
		public double[] getPixelResolution() { return parseDoubleArray( pixelResolution ); }

		private static double[] parseDoubleArray( final String str )
		{
			if ( str == null )
				return null;

			final String[] tokens = str.split( "," );
			final double[] values = new double[ tokens.length ];
			for ( int i = 0; i < values.length; i++ )
				values[ i ] = Double.parseDouble( tokens[ i ] );
			return values;
		}
	}
}
