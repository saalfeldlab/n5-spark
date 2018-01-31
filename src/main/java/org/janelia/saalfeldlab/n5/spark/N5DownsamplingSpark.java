package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.imglib2.list.N5SerializableUtils;
import org.janelia.saalfeldlab.n5.spark.N5DownsamplingSpark.IsotropicScalingEstimator.IsotropicScalingParameters;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodFactory;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodUnsafe;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.operators.Add;
import net.imglib2.type.operators.MulFloatingPoint;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

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
			final int[] cellSize = voxelDimensions != null ? isotropicScalingParameters.cellSize : fullScaleCellSize;
			final int[] downsamplingFactors = isotropicScalingParameters.downsamplingFactors;

			final long[] downsampledDimensions = fullScaleDimensions.clone();
			for ( int d = 0; d < downsampledDimensions.length; ++d )
				downsampledDimensions[ d ] /= downsamplingFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 || Arrays.stream( downsampledDimensions ).max().getAsLong() < Arrays.stream( cellSize ).max().getAsInt() )
				break;

			final int[] relativeDownsamplingFactors = new int[ downsamplingFactors.length ];
			for ( int d = 0; d < downsamplingFactors.length; ++d )
				relativeDownsamplingFactors[ d ] = downsamplingFactors[ d ] / scales.get( scale - 1 )[ d ];

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

				runDownsampleTask(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						outputDatasetPath,
						relativeDownsamplingFactors
					);
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

				runDownsampleTask(
						sparkContext,
						n5Supplier,
						inputXYDatasetPath,
						outputXYDatasetPath,
						new int[] { relativeDownsamplingFactors[ 0 ], relativeDownsamplingFactors[ 1 ], 1 }
					);

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

				runDownsampleTask(
						sparkContext,
						n5Supplier,
						inputDatasetPath,
						outputDatasetPath,
						new int[] { 1, 1, downsamplingFactors[ 2 ] }
					);
			}

			final String outputDatasetPath = Paths.get( rootOutputPath, "s" + scale ).toString();
			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, downsamplingFactors );
			scales.add( downsamplingFactors );
		}

		if ( needIntermediateDownsamplingInXY )
			N5RemoveSpark.remove( sparkContext, n5Supplier, xyGroupPath );

		return scales.toArray( new int[ 0 ][] );
	}

	@SuppressWarnings( { "rawtypes", "unchecked" } )
	private static < T extends Type< T > & Add< T > & MulFloatingPoint > void runDownsampleTask(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes outputAttributes = n5.getDatasetAttributes( outputDatasetPath );
		final long[] outputDimensions = outputAttributes.getDimensions();
		final int[] outputCellSize = outputAttributes.getBlockSize();
		final int dim = outputCellSize.length;

		final CellGrid outputCellGrid = new CellGrid( outputDimensions, outputCellSize );
		final long numDownsampledBlocks = Intervals.numElements( outputCellGrid.getGridDimensions() );
		final List< Long > blockIndexes = LongStream.range( 0, numDownsampledBlocks ).boxed().collect( Collectors.toList() );

		sparkContext.parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) ).foreach( blockIndex ->
		{
			final CellGrid cellGrid = new CellGrid( outputDimensions, outputCellSize );
			final long[] gridPosition = new long[ dim ];
			cellGrid.getCellGridPositionFlat( blockIndex, gridPosition );

			final long[] sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			final int[] cellDimensions = new int[ dim ];
			cellGrid.getCellDimensions( gridPosition, targetMin, cellDimensions );
			for ( int d = 0; d < dim; ++d )
			{
				targetMax[ d ] = targetMin[ d ] + cellDimensions[ d ] - 1;
				sourceMin[ d ] = targetMin[ d ] * downsamplingFactors[ d ];
				sourceMax[ d ] = targetMax[ d ] * downsamplingFactors[ d ] + ( downsamplingFactors[ d ] - 1 );
			}

			final Interval sourceInterval = new FinalInterval( sourceMin, sourceMax );
			final Interval targetInterval = new FinalInterval( targetMin, targetMax );

			final N5Writer n5Local = n5Supplier.get();

			final RandomAccessibleInterval< T > previousScaleLevelImg;
			if ( !DataType.SERIALIZABLE.equals( n5Local.getDatasetAttributes( inputDatasetPath ).getDataType() ) )
				previousScaleLevelImg = ( RandomAccessibleInterval ) N5Utils.open( n5Local, inputDatasetPath );
			else
				previousScaleLevelImg = ( RandomAccessibleInterval ) N5SerializableUtils.open( n5Local, inputDatasetPath );

			final RandomAccessibleInterval< T > source = Views.interval( previousScaleLevelImg, sourceInterval );
			final T type = Util.getTypeFromInterval( source );

			final ImgFactory< T > imgFactory;
			if ( NativeType.class.isInstance( type ) )
				imgFactory = ( ImgFactory ) new ArrayImgFactory<>();
			else
				imgFactory = new ListImgFactory<>();

			final Img< T > target = imgFactory.create( targetInterval, type );
			downsampleImpl( source, Views.translate( target, targetMin ), downsamplingFactors );

			if ( !DataType.SERIALIZABLE.equals( n5Local.getDatasetAttributes( outputDatasetPath ).getDataType() ) )
				N5Utils.saveBlock( ( RandomAccessibleInterval ) target, n5Local, outputDatasetPath, gridPosition );
			else
				N5SerializableUtils.saveBlock( ( RandomAccessibleInterval ) target, n5Local, outputDatasetPath, gridPosition );
		} );
	}

	/**
	 *  adapted from bdv.export.Downsample
	 */
	@SuppressWarnings( { "rawtypes" } )
	private static < T extends Type< T > & Add< T > & MulFloatingPoint > void downsampleImpl(
			final RandomAccessible< T > input,
			final RandomAccessibleInterval< T > output,
			final int[] factor )
	{
		assert input.numDimensions() == output.numDimensions();
		assert input.numDimensions() == factor.length;

		final int n = input.numDimensions();
		final RectangleNeighborhoodFactory< T > f = RectangleNeighborhoodUnsafe.< T >factory();
		final long[] dim = new long[ n ];
		for ( int d = 0; d < n; ++d )
			dim[ d ] = factor[ d ];
		final Interval spanInterval = new FinalInterval( dim );

		final long[] minRequiredInput = new long[ n ];
		final long[] maxRequiredInput = new long[ n ];
		output.min( minRequiredInput );
		output.max( maxRequiredInput );
		for ( int d = 0; d < n; ++d )
		{
			minRequiredInput[ d ] *= factor[ d ];
			maxRequiredInput[ d ] *= factor[ d ];
			maxRequiredInput[ d ] += factor[ d ] - 1;
		}
		final RandomAccessibleInterval< T > requiredInput = Views.interval( input, new FinalInterval( minRequiredInput, maxRequiredInput ) );

		final RectangleShape.NeighborhoodsAccessible< T > neighborhoods = new RectangleShape.NeighborhoodsAccessible<>( requiredInput, spanInterval, f );
		final RandomAccess< Neighborhood< T > > block = neighborhoods.randomAccess();

		long size = 1;
		for ( int d = 0; d < n; ++d )
			size *= factor[ d ];
		final double scale = 1.0 / size;

		final Cursor< T > out = Views.iterable( output ).localizingCursor();
		while( out.hasNext() )
		{
			final T o = out.next();
			if ( RealType.class.isInstance( o ) )
			{
				double sum = 0;
				for ( int d = 0; d < n; ++d )
					block.setPosition( out.getLongPosition( d ) * factor[ d ], d );
				for ( final Object i : ( Neighborhood ) block.get() )
					sum += ( ( RealType ) i ).getRealDouble();
				( ( RealType< ? > ) o ).setReal( sum * scale );
			}
			else
			{
				for ( int d = 0; d < n; ++d )
					block.setPosition( out.getLongPosition( d ) * factor[ d ], d );
				for ( final T i : block.get() )
					o.add( i );
				o.mul( scale );
			}
		}
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
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
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

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -1467734459169624759L;

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
