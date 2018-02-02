package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5DownsamplerSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Downsamples the given input dataset of an N5 container with respect to the given downsampling factors.
	 * The output dataset will be created within the same N5 container.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void downsample(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors ) throws IOException
	{
		downsampleWithOffset(
				sparkContext,
				n5Supplier,
				inputDatasetPath,
				outputDatasetPath,
				downsamplingFactors,
				new long[ downsamplingFactors.length ]
			);
	}

	/**
	 * Downsamples the given input dataset of an N5 container with respect to the given downsampling factors
	 * and the given offset. The output dataset will be created within the same N5 container.
	 *
	 * For example, if the input dataset dimensions are [9], the downsampling factor is [4], and the offset is [3],
	 * the resulting accumulated pixels will be [(0),(1,2,3,4),(5,6,7,8)].
	 * When downsampling without the offset in the same example, the result will be [(0,1,2,3),(4,5,6,7)].
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @param offset
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void downsampleWithOffset(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors,
			final long[] offset ) throws IOException
	{
		if ( Arrays.stream( downsamplingFactors ).max().getAsInt() <= 1 )
			throw new IllegalArgumentException( "Invalid downsampling factors: " + Arrays.toString( downsamplingFactors ) );

		final N5Writer n5 = n5Supplier.get();
		if ( !n5.datasetExists( inputDatasetPath ) )
			throw new IllegalArgumentException( "Input N5 dataset " + inputDatasetPath + " does not exist" );
		if ( n5.datasetExists( outputDatasetPath ) )
			throw new IllegalArgumentException( "Output N5 dataset " + outputDatasetPath + " already exists" );

		final DatasetAttributes inputAttributes = n5.getDatasetAttributes( inputDatasetPath );
		final int dim = inputAttributes.getNumDimensions();

		if ( dim != downsamplingFactors.length || dim != offset.length )
			throw new IllegalArgumentException( "Downsampling parameters do not match data dimensionality." );

		final long[] outputDimensions = new long[ dim ];
		for ( int d = 0; d < dim; ++d )
			outputDimensions[ d ] = ( inputAttributes.getDimensions()[ d ] + offset[ d ] ) / downsamplingFactors[ d ];

		if ( Arrays.stream( outputDimensions ).min().getAsLong() < 1 )
			throw new IllegalArgumentException( "Degenerate output dimensions: " + Arrays.toString( outputDimensions ) );

		final int[] outputBlockSize = inputAttributes.getBlockSize().clone();
		n5.createDataset(
				outputDatasetPath,
				outputDimensions,
				outputBlockSize,
				inputAttributes.getDataType(),
				inputAttributes.getCompression()
			);

		final CellGrid outputCellGrid = new CellGrid( outputDimensions, outputBlockSize );
		final long numDownsampledBlocks = Intervals.numElements( outputCellGrid.getGridDimensions() );
		final List< Long > blockIndexes = LongStream.range( 0, numDownsampledBlocks ).boxed().collect( Collectors.toList() );

		sparkContext.parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) ).foreach( blockIndex ->
		{
			final CellGrid cellGrid = new CellGrid( outputDimensions, outputBlockSize );
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( blockIndex, blockGridPosition );

			final long[] sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			final int[] cellDimensions = new int[ dim ];
			cellGrid.getCellDimensions( blockGridPosition, targetMin, cellDimensions );
			for ( int d = 0; d < dim; ++d )
			{
				targetMax[ d ] = targetMin[ d ] + cellDimensions[ d ] - 1;
				sourceMin[ d ] = targetMin[ d ] * downsamplingFactors[ d ];
				sourceMax[ d ] = targetMax[ d ] * downsamplingFactors[ d ] + downsamplingFactors[ d ] - 1;
			}
			final Interval sourceInterval = new FinalInterval( sourceMin, sourceMax );
			final Interval targetInterval = new FinalInterval( targetMin, targetMax );

			final N5Writer n5Local = n5Supplier.get();
			final RandomAccessibleInterval< T > source = N5Utils.open( n5Local, inputDatasetPath );
			final RandomAccessibleInterval< T > targetBlock = new ArrayImgFactory< T >().create( targetInterval, Util.getTypeFromInterval( source ) );

//			if ( Intervals.contains( translatedSource, sourceInterval ) )
//			{
				// source interval is fully contained in the image, use simple downsampler
//				final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, sourceInterval );
//				downsampleInterval( sourceBlock, targetBlock, downsamplingFactors );
//			}
//			else
			{
//				final ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > extendedSource = Views.extendZero( source );
//				final RandomAccessible< T > translatedExtendedSource = Views.translate( extendedSource, offset );
//				final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( translatedExtendedSource, sourceInterval );
				final RandomAccessibleInterval< T > translatedSource = Views.translate( source, offset );
				final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( translatedSource, sourceInterval );

//				final Interval translatedSourceInterval = new FinalInterval( translatedSource );
				final long[] definedSourceMin = new long[ dim ], definedSourceMax = new long[ dim ];
				for ( int d = 0; d < dim; ++d )
				{
					definedSourceMin[ d ] = Math.max( translatedSource.min( d ) - sourceMin[ d ], 0 );
					definedSourceMax[ d ] = translatedSource.dimension( d ) - 1 - sourceMin[ d ] + offset[ d ];
				}
				final Interval definedSourceInterval = new FinalInterval( definedSourceMin, definedSourceMax );

				downsampleIntervalWithOutOfBoundsCheck( sourceBlock, targetBlock, downsamplingFactors, definedSourceInterval );
			}

			N5Utils.saveBlock( targetBlock, n5Local, outputDatasetPath, blockGridPosition );
		} );
	}

	/**
	 * Based on the code from bdv.export.Downsample class.
	 */
	/*private static < T extends RealType< T > > void downsampleInterval(
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
			for ( int d = 0; d < n; ++d )
				block.setPosition( out.getLongPosition( d ) * factor[ d ], d );
			double sum = 0;
			for ( final T i : block.get() )
				sum += i.getRealDouble();
			o.setReal( sum * scale );
		}
	}*/

	private static < T extends RealType< T > > void downsampleIntervalWithOutOfBoundsCheck(
			final RandomAccessible< T > input,
			final RandomAccessibleInterval< T > output,
			final int[] factor,
			final Interval definedInputInterval )
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

		final Cursor< T > out = Views.iterable( output ).localizingCursor();
		while( out.hasNext() )
		{
			final T o = out.next();
			for ( int d = 0; d < n; ++d )
				block.setPosition( out.getLongPosition( d ) * factor[ d ], d );
			final Cursor< T > neighborhoodCursor = block.get().localizingCursor();

			double sum = 0;
			long count = 0;
			while ( neighborhoodCursor.hasNext() )
			{
				final T i = neighborhoodCursor.next();
				if ( Intervals.contains( definedInputInterval, neighborhoodCursor ) )
				{
					++count;
					sum += i.getRealDouble();
				}
			}
			final double scale = 1.0 / count;
			o.setReal( sum * scale );
		}
	}


	public static void main( final String... args ) throws IOException, CmdLineException
	{
		final Arguments parsedArgs = new Arguments( args );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5DownsamplerSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
			if ( parsedArgs.getOffset() != null )
			{
				downsampleWithOffset(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getOutputDatasetPath(),
						parsedArgs.getDownsamplingFactors(),
						parsedArgs.getOffset()
					);
			}
			else
			{
				downsample(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getOutputDatasetPath(),
						parsedArgs.getDownsamplingFactors()
					);
			}
		}
		System.out.println( "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -1467734459169624759L;

		@Option(name = "-n", aliases = { "--n5" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--output" }, required = true,
				usage = "Path to the output dataset to be created (e.g. data/group/s1).")
		private String outputDatasetPath;

		@Option(name = "-f", aliases = { "--factor" }, required = true,
				usage = "Downsampling factors.")
		private String downsamplingFactors;

		@Option(name = "--offset", required = false,
				usage = "Offset.")
		private String offset;

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
		public String getOutputDatasetPath() { return outputDatasetPath; }
		public int[] getDownsamplingFactors() { return parseIntArray( downsamplingFactors ); }
		public long[] getOffset() { return parseLongArray( offset ); }

		private static int[] parseIntArray( final String str )
		{
			if ( str == null )
				return null;

			final String[] tokens = str.split( "," );
			final int[] values = new int[ tokens.length ];
			for ( int i = 0; i < values.length; i++ )
				values[ i ] = Integer.parseInt( tokens[ i ] );
			return values;
		}

		private static long[] parseLongArray( final String str )
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
}
