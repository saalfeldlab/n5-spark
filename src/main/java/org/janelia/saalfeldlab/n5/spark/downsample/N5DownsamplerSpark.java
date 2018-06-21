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
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import bdv.export.Downsample;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5DownsamplerSpark
{
	public static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";

	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Downsamples the given input dataset of an N5 container with respect to the given downsampling factors.
	 * The output dataset will be created within the same N5 container with the same block size as the input dataset.
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
		downsample(
				sparkContext,
				n5Supplier,
				inputDatasetPath,
				outputDatasetPath,
				downsamplingFactors,
				null
			);
	}

	/**
	 * Downsamples the given input dataset of an N5 container with respect to the given downsampling factors.
	 * The output dataset will be created within the same N5 container with given block size.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @param blockSize
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void downsample(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors,
			final int[] blockSize ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		if ( !n5.datasetExists( inputDatasetPath ) )
			throw new IllegalArgumentException( "Input N5 dataset " + inputDatasetPath + " does not exist" );
		if ( n5.datasetExists( outputDatasetPath ) )
			throw new IllegalArgumentException( "Output N5 dataset " + outputDatasetPath + " already exists" );

		final DatasetAttributes inputAttributes = n5.getDatasetAttributes( inputDatasetPath );
		final int dim = inputAttributes.getNumDimensions();

		if ( dim != downsamplingFactors.length )
			throw new IllegalArgumentException( "Downsampling parameters do not match data dimensionality." );

		final long[] outputDimensions = new long[ dim ];
		for ( int d = 0; d < dim; ++d )
			outputDimensions[ d ] = inputAttributes.getDimensions()[ d ] / downsamplingFactors[ d ];

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

		// set the downsampling factors attribute
		final int[] inputAbsoluteDownsamplingFactors = n5.getAttribute( inputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class );
		final int[] outputAbsoluteDownsamplingFactors = new int[ downsamplingFactors.length ];
		for ( int d = 0; d < downsamplingFactors.length; ++d )
			outputAbsoluteDownsamplingFactors[ d ] = downsamplingFactors[ d ] * ( inputAbsoluteDownsamplingFactors != null ? inputAbsoluteDownsamplingFactors[ d ] : 1 );
		n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, outputAbsoluteDownsamplingFactors );

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
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, sourceInterval );

			/* test if empty */
			final T defaultValue = Util.getTypeFromInterval( sourceBlock ).createVariable();
			boolean isEmpty = true;
			for ( final T t : Views.iterable( sourceBlock ) )
			{
				isEmpty &= defaultValue.valueEquals( t );
				if ( !isEmpty ) break;
			}
			if ( isEmpty )
				return;

			/* do if not empty */
			final RandomAccessibleInterval< T > targetBlock = new ArrayImgFactory< T >().create( targetInterval, defaultValue );
			Downsample.downsample( sourceBlock, targetBlock, downsamplingFactors );

			N5Utils.saveNonEmptyBlock( targetBlock, n5Local, outputDatasetPath, blockGridPosition, defaultValue );
		} );
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

			final String[] outputDatasetPath = parsedArgs.getOutputDatasetPath();
			final int[][] downsamplingFactors = parsedArgs.getDownsamplingFactors();

			if ( outputDatasetPath.length != downsamplingFactors.length )
				throw new IllegalArgumentException( "Number of output datasets does not match downsampling factors!" );

			downsample(
					sparkContext,
					n5Supplier,
					parsedArgs.getInputDatasetPath(),
					outputDatasetPath[ 0 ],
					downsamplingFactors[ 0 ],
					parsedArgs.getBlockSize()
				);

			for ( int i = 1; i < downsamplingFactors.length; i++ )
			{
				downsample(
						sparkContext,
						n5Supplier,
						outputDatasetPath[ i - 1 ],
						outputDatasetPath[ i ],
						downsamplingFactors[ i ],
						parsedArgs.getBlockSize()
					);
			}
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

		@Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true, handler = StringArrayOptionHandler.class,
				usage = "Path(s) to the output dataset to be created (e.g. data/group/s1).")
		private String[] outputDatasetPath;

		@Option(name = "-f", aliases = { "--factors" }, required = true, handler = StringArrayOptionHandler.class,
				usage = "Downsampling factors. If using multiple, each factor builds on the last.")
		private String[] downsamplingFactors;

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
		public String[] getOutputDatasetPath() { return outputDatasetPath; }
		public int[][] getDownsamplingFactors() { return CmdUtils.parseMultipleIntArrays( downsamplingFactors ); }
		public int[] getBlockSize() { return CmdUtils.parseIntArray( blockSize ); }
	}
}
