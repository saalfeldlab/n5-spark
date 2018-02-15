package org.janelia.saalfeldlab.n5.spark.downsample;

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
import org.janelia.saalfeldlab.n5.spark.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import bdv.export.Downsample;
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

public class N5OffsetDownsamplerSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Downsamples the given input dataset with respect to the given downsampling factors and the given offset.
	 * The output dataset will be created within the same N5 container with the same block size as the input dataset.
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
		downsampleWithOffset(
				sparkContext,
				n5Supplier,
				inputDatasetPath,
				outputDatasetPath,
				downsamplingFactors,
				offset,
				null
			);
	}

	/**
	 * Downsamples the given input dataset with respect to the given downsampling factors and the given offset.
	 * The output dataset will be created within the same N5 container with given block size.
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
	 * @param blockSize
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void downsampleWithOffset(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors,
			final long[] offset,
			final int[] blockSize ) throws IOException
	{
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

		final int[] outputBlockSize = blockSize != null ? blockSize : inputAttributes.getBlockSize();
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
			// downsampled block index to grid position
			final CellGrid cellGrid = new CellGrid( outputDimensions, outputBlockSize );
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( blockIndex, blockGridPosition );

			// find corresponding source interval
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

			// apply offset to source to align it with respect to the target block
			final RandomAccessibleInterval< T > translatedSource = Views.translate( source, offset );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( translatedSource, sourceInterval );

			// now that the source block is aligned, find the interval where it is defined within the target block
			final long[] definedSourceBlockMin = new long[ dim ], definedSourceBlockMax = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
			{
				definedSourceBlockMin[ d ] = Math.max( translatedSource.min( d ) - sourceMin[ d ], 0 );
				definedSourceBlockMax[ d ] = Math.min( translatedSource.max( d ), sourceMax[ d ] ) - sourceMin[ d ];
			}
			final Interval definedSourceBlockInterval = new FinalInterval( definedSourceBlockMin, definedSourceBlockMax );
			final RandomAccessibleInterval< T > definedSourceBlock = Views.interval( sourceBlock, definedSourceBlockInterval );

			/* test if empty */
			final T defaultValue = Util.getTypeFromInterval( source ).createVariable();
			boolean isEmpty = true;
			for ( final T t : Views.iterable( definedSourceBlock ) )
			{
				isEmpty &= defaultValue.valueEquals( t );
				if ( !isEmpty ) break;
			}
			if ( isEmpty )
				return;

			/* do if not empty */
			final RandomAccessibleInterval< T > targetBlock = new ArrayImgFactory< T >().create( targetInterval, defaultValue );

			if ( Intervals.equalDimensions( definedSourceBlockInterval, sourceInterval ) )
				Downsample.downsample( sourceBlock, targetBlock, downsamplingFactors );
			else
				downsampleIntervalOutOfBoundsCheck( sourceBlock, targetBlock, downsamplingFactors, definedSourceBlockInterval );

			N5Utils.saveNonEmptyBlock( targetBlock, n5Local, outputDatasetPath, blockGridPosition, defaultValue );
		} );
	}

	/**
	 * Based on {@link bdv.export.Downsample}.
	 */
	private static < T extends RealType< T > > void downsampleIntervalOutOfBoundsCheck(
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
				.setAppName( "N5OffsetDownsamplerSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
			downsampleWithOffset(
					sparkContext,
					n5Supplier,
					parsedArgs.getInputDatasetPath(),
					parsedArgs.getOutputDatasetPath(),
					parsedArgs.getDownsamplingFactors(),
					parsedArgs.getOffset(),
					parsedArgs.getBlockSize()
				);
		}
		System.out.println( "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -1467734459169624759L;

		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
				usage = "Path to the output dataset to be created (e.g. data/group/s1).")
		private String outputDatasetPath;

		@Option(name = "-f", aliases = { "--factors" }, required = true,
				usage = "Downsampling factors.")
		private String downsamplingFactors;

		@Option(name = "-s", aliases = { "--offset" }, required = true,
				usage = "Offset.")
		private String offset;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Block size for the output dataset (by default same as for input dataset).")
		private String blockSize;

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
		public int[] getDownsamplingFactors() { return CmdUtils.parseIntArray( downsamplingFactors ); }
		public int[] getBlockSize() { return CmdUtils.parseIntArray( blockSize ); }
		public long[] getOffset() { return CmdUtils.parseLongArray( offset ); }
	}
}
