package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5Compression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class N5ConvertSpark
{
	private static final int MAX_PARTITIONS = 15000;

	static class ClampingConverter< I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > implements Converter< I, O >
	{
		private final double minInputValue, maxInputValue;
		private final double minOutputValue, maxOutputValue;
		private final double inputValueRange, outputValueRange;

		public ClampingConverter(
				final double minInputValue, final double maxInputValue,
				final double minOutputValue, final double maxOutputValue )
		{
			this.minInputValue = minInputValue; this.maxInputValue = maxInputValue;
			this.minOutputValue = minOutputValue; this.maxOutputValue = maxOutputValue;

			inputValueRange = maxInputValue - minInputValue;
			outputValueRange = maxOutputValue - minOutputValue;
		}

		@Override
		public void convert( final I input, final O output )
		{
			final double inputValue = input.getRealDouble();
			if ( inputValue <= minInputValue )
			{
				output.setReal( minOutputValue );
			}
			else if ( inputValue >= maxInputValue )
			{
				output.setReal( maxOutputValue );
			}
			else
			{
				final double normalizedInputValue = ( inputValue - minInputValue ) / inputValueRange;
				final double realOutputValue = normalizedInputValue * outputValueRange + minOutputValue;
				output.setReal( realOutputValue );
			}
		}
	}

	public static < I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5InputSupplier,
			final String inputDatasetPath,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath,
			final Optional< int[] > blockSizeOptional,
			final Optional< Compression > compressionOptional,
			final Optional< DataType > dataTypeOptional,
			final Optional< Pair< Double, Double > > valueRangeOptional ) throws IOException
	{
		convert(
				sparkContext,
				n5InputSupplier,
				inputDatasetPath,
				n5OutputSupplier,
				outputDatasetPath,
				blockSizeOptional,
				compressionOptional,
				dataTypeOptional,
				valueRangeOptional,
				false );
	}

	public static < I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5InputSupplier,
			final String inputDatasetPath,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath,
			final Optional< int[] > blockSizeOptional,
			final Optional< Compression > compressionOptional,
			final Optional< DataType > dataTypeOptional,
			final Optional< Pair< Double, Double > > valueRangeOptional,
			final boolean overwriteExisting ) throws IOException
	{
		final N5Reader n5Input = n5InputSupplier.get();
		final DatasetAttributes inputAttributes = n5Input.getDatasetAttributes( inputDatasetPath );

		final int[] inputBlockSize = inputAttributes.getBlockSize();
		final Compression inputCompression = inputAttributes.getCompression();
		final DataType inputDataType = inputAttributes.getDataType();

		final N5Writer n5Output = n5OutputSupplier.get();
		if ( !overwriteExisting && n5Output.datasetExists( outputDatasetPath ) )
			throw new RuntimeException( "Output dataset already exists: " + outputDatasetPath );

		final int[] outputBlockSize = blockSizeOptional.isPresent() ? blockSizeOptional.get() : inputBlockSize;
		final Compression outputCompression = compressionOptional.isPresent() ? compressionOptional.get() : inputCompression;
		final DataType outputDataType = dataTypeOptional.isPresent() ? dataTypeOptional.get() : inputDataType;

		final long[] dimensions = inputAttributes.getDimensions();
		n5Output.createDataset( outputDatasetPath, dimensions, outputBlockSize, outputDataType, outputCompression );

		// derive input and output value range
		final double minInputValue, maxInputValue;
		if ( valueRangeOptional.isPresent() )
		{
			minInputValue = valueRangeOptional.get().getA();
			maxInputValue = valueRangeOptional.get().getB();
		}
		else
		{
			if ( inputDataType == DataType.FLOAT32 || inputDataType == DataType.FLOAT64 )
			{
				minInputValue = 0;
				maxInputValue = 1;
			}
			else
			{
				final I inputType = N5Utils.type( inputDataType );
				minInputValue = inputType.getMinValue();
				maxInputValue = inputType.getMaxValue();
			}
		}

		final double minOutputValue, maxOutputValue;
		if ( outputDataType == DataType.FLOAT32 || outputDataType == DataType.FLOAT64 )
		{
			minOutputValue = 0;
			maxOutputValue = 1;
		}
		else
		{
			final O outputType = N5Utils.type( outputDataType );
			minOutputValue = outputType.getMinValue();
			maxOutputValue = outputType.getMaxValue();
		}

		System.out.println( "Input value range: " + Arrays.toString( new double[] { minInputValue, maxInputValue } ) );
		System.out.println( "Output value range: " + Arrays.toString( new double[] { minOutputValue, maxOutputValue } ) );

		if ( Intervals.numElements( outputBlockSize ) >= Intervals.numElements( inputBlockSize ) )
		{
			System.out.println( "Output block size is the same or bigger than the input block size, parallelizing over output blocks..." );
			convertParallelizingOverOutputBlocks(
					sparkContext,
					n5InputSupplier,
					inputDatasetPath,
					n5OutputSupplier,
					outputDatasetPath,
					minInputValue, maxInputValue,
					minOutputValue, maxOutputValue
				);
		}
		else
		{
			System.out.println( "Output block size is smaller than the input block size, parallelizing over adjusted input blocks..." );
			convertParallelizingOverAdjustedInputBlocks(
					sparkContext,
					n5InputSupplier,
					inputDatasetPath,
					n5OutputSupplier,
					outputDatasetPath,
					minInputValue, maxInputValue,
					minOutputValue, maxOutputValue
				);
		}
	}

	@SuppressWarnings( "unchecked" )
	private static < I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > void convertParallelizingOverOutputBlocks(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5InputSupplier,
			final String inputDatasetPath,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath,
			final double minInputValue, final double maxInputValue,
			final double minOutputValue, final double maxOutputValue ) throws IOException
	{
		final DatasetAttributes inputAttributes = n5InputSupplier.get().getDatasetAttributes( inputDatasetPath );
		final long[] dimensions = inputAttributes.getDimensions();
		final DataType inputDataType = inputAttributes.getDataType();

		final DatasetAttributes outputAttributes = n5OutputSupplier.get().getDatasetAttributes( outputDatasetPath );
		final int[] outputBlockSize = outputAttributes.getBlockSize();
		final DataType outputDataType = outputAttributes.getDataType();

		final long numOutputBlocks = Intervals.numElements( new CellGrid( dimensions, outputBlockSize ).getGridDimensions() );
		final List< Long > outputBlockIndexes = LongStream.range( 0, numOutputBlocks ).boxed().collect( Collectors.toList() );

		sparkContext.parallelize( outputBlockIndexes, Math.min( outputBlockIndexes.size(), MAX_PARTITIONS ) ).foreach( outputBlockIndex ->
		{
			final CellGrid outputBlockGrid = new CellGrid( dimensions, outputBlockSize );
			final long[] outputBlockGridPosition = new long[ outputBlockGrid.numDimensions() ];
			outputBlockGrid.getCellGridPositionFlat( outputBlockIndex, outputBlockGridPosition );

			final long[] outputBlockMin = new long[ outputBlockGrid.numDimensions() ], outputBlockMax = new long[ outputBlockGrid.numDimensions() ];
			final int[] outputBlockDimensions = new int[ outputBlockGrid.numDimensions() ];
			outputBlockGrid.getCellDimensions( outputBlockGridPosition, outputBlockMin, outputBlockDimensions );
			for ( int d = 0; d < outputBlockGrid.numDimensions(); ++d )
				outputBlockMax[ d ] = outputBlockMin[ d ] + outputBlockDimensions[ d ] - 1;
			final Interval outputBlockInterval = new FinalInterval( outputBlockMin, outputBlockMax );

			final O outputType = N5Utils.type( outputDataType );

			final RandomAccessibleInterval< I > source = N5Utils.open( n5InputSupplier.get(), inputDatasetPath );
			final RandomAccessible< O > convertedSource;
			if ( inputDataType == outputDataType )
			{
				convertedSource = ( RandomAccessible< O > ) source;
			}
			else
			{
				convertedSource = Converters.convert( source, new ClampingConverter< I, O >(
						minInputValue, maxInputValue,
						minOutputValue, maxOutputValue
					), outputType.createVariable() );
			}
			final RandomAccessibleInterval< O > convertedSourceInterval = Views.offsetInterval( convertedSource, outputBlockInterval );

			// Empty blocks will not be written out.
			// Delete blocks to avoid remnant blocks if overwriting.
			deleteBlock(convertedSourceInterval, n5OutputSupplier.get(), outputDatasetPath, outputBlockSize, outputBlockGridPosition);
			N5Utils.saveNonEmptyBlock(
				convertedSourceInterval,
				n5OutputSupplier.get(),
				outputDatasetPath,
				outputBlockGridPosition,
				outputType.createVariable()
			);
		} );
	}

	@SuppressWarnings( "unchecked" )
	private static < I extends NativeType< I > & RealType< I >, O extends NativeType< O > & RealType< O > > void convertParallelizingOverAdjustedInputBlocks(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5InputSupplier,
			final String inputDatasetPath,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath,
			final double minInputValue, final double maxInputValue,
			final double minOutputValue, final double maxOutputValue ) throws IOException
	{
		final DatasetAttributes inputAttributes = n5InputSupplier.get().getDatasetAttributes( inputDatasetPath );
		final long[] dimensions = inputAttributes.getDimensions();
		final int[] inputBlockSize = inputAttributes.getBlockSize();
		final DataType inputDataType = inputAttributes.getDataType();

		final DatasetAttributes outputAttributes = n5OutputSupplier.get().getDatasetAttributes( outputDatasetPath );
		final int[] outputBlockSize = outputAttributes.getBlockSize();
		final DataType outputDataType = outputAttributes.getDataType();

		// adjust the size of the processing block to minimize number of reads of each input block
		final int[] adjustedBlockSize = new int[ inputBlockSize.length ];
		for ( int d = 0; d < adjustedBlockSize.length; ++d )
			adjustedBlockSize[ d ] = ( int ) Math.max( Math.round( ( double ) inputBlockSize[ d ] / outputBlockSize[ d ] ), 1) * outputBlockSize[ d ];

		final long numAdjustedBlocks = Intervals.numElements( new CellGrid( dimensions, adjustedBlockSize ).getGridDimensions() );
		final List< Long > adjustedBlockIndexes = LongStream.range( 0, numAdjustedBlocks ).boxed().collect( Collectors.toList() );

		sparkContext.parallelize( adjustedBlockIndexes, Math.min( adjustedBlockIndexes.size(), MAX_PARTITIONS ) ).foreach( adjustedBlockIndex ->
		{
			final CellGrid adjustedBlockGrid = new CellGrid( dimensions, adjustedBlockSize );
			final long[] adjustedBlockGridPosition = new long[ adjustedBlockGrid.numDimensions() ];
			adjustedBlockGrid.getCellGridPositionFlat( adjustedBlockIndex, adjustedBlockGridPosition );

			final long[] adjustedBlockMin = new long[ adjustedBlockGrid.numDimensions() ], adjustedBlockMax = new long[ adjustedBlockGrid.numDimensions() ];
			final int[] adjustedBlockDimensions = new int[ adjustedBlockGrid.numDimensions() ];
			adjustedBlockGrid.getCellDimensions( adjustedBlockGridPosition, adjustedBlockMin, adjustedBlockDimensions );
			for ( int d = 0; d < adjustedBlockGrid.numDimensions(); ++d )
				adjustedBlockMax[ d ] = adjustedBlockMin[ d ] + adjustedBlockDimensions[ d ] - 1;
			final Interval adjustedBlockInterval = new FinalInterval( adjustedBlockMin, adjustedBlockMax );

			final O outputType = N5Utils.type( outputDataType );

			final RandomAccessibleInterval< I > source = N5Utils.open( n5InputSupplier.get(), inputDatasetPath );
			final RandomAccessible< O > convertedSource;
			if ( inputDataType == outputDataType )
			{
				convertedSource = ( RandomAccessible< O > ) source;
			}
			else
			{
				convertedSource = Converters.convert( source, new ClampingConverter< I, O >(
						minInputValue, maxInputValue,
						minOutputValue, maxOutputValue
					), outputType.createVariable() );
			}
			final RandomAccessibleInterval< O > convertedSourceInterval = Views.offsetInterval( convertedSource, adjustedBlockInterval );

			// compute correct output block grid offset
			final CellGrid outputBlockGrid = new CellGrid( dimensions, outputBlockSize );
			final long[] outputBlockGridPosition = new long[ outputBlockGrid.numDimensions() ];
			outputBlockGrid.getCellPosition( adjustedBlockMin, outputBlockGridPosition );


			// Empty blocks will not be written out.
			// Delete blocks to avoid remnant blocks if overwriting.
			deleteBlock(convertedSourceInterval, n5OutputSupplier.get(), outputDatasetPath, outputBlockSize, outputBlockGridPosition);
			N5Utils.saveNonEmptyBlock(
				convertedSourceInterval,
				n5OutputSupplier.get(),
				outputDatasetPath,
				outputBlockGridPosition,
				outputType.createVariable()
			);
		} );
	}

	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5ConvertSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			convert(
					sparkContext,
					() -> new N5FSReader( parsedArgs.getInputN5Path() ),
					parsedArgs.getInputDatasetPath(),
					() -> new N5FSWriter( parsedArgs.getOutputN5Path() ),
					parsedArgs.getOutputDatasetPath(),
					Optional.ofNullable( parsedArgs.getBlockSize() ),
					Optional.ofNullable( parsedArgs.getCompression() ),
					Optional.ofNullable( parsedArgs.getDataType() ),
					Optional.ofNullable( parsedArgs.getValueRange() ),
					parsedArgs.force
				);
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = 4847292347478989514L;

		@Option(name = "-ni", aliases = { "-n", "--inputN5Path" }, required = true,
				usage = "Path to the input N5 container.")
		private String n5InputPath;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-no", aliases = { "--outputN5Path" }, required = false,
				usage = "Path to the output N5 container (by default the output dataset is stored within the same container as the input dataset).")
		private String n5OutputPath;

		@Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
				usage = "Output dataset path.")
		private String outputDatasetPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Block size for the output dataset (by default the same block size is used as for the input dataset).")
		private String blockSizeStr;

		@Option(name = "-c", aliases = { "--compression" }, required = false,
				usage = "Compression to be used for the converted dataset (by default the same compression is used as for the input dataset).")
		private N5Compression n5Compression;

		@Option(name = "-t", aliases = { "--type" }, required = false,
				usage = "Type to be used for the converted dataset (by default the same type is used as for the input dataset)."
						+ "If a different type is used, the values are mapped to the range of the output type, rounding to the nearest integer value if necessary.")
		private DataType dataType;

		@Option(name = "-min", aliases = { "--minValue" }, required = false,
				usage = "Minimum value of the input range to be used for the conversion (default is min type value for integer types, or 0 for real types).")
		private Double minValue;

		@Option(name = "-max", aliases = { "--maxValue" }, required = false,
				usage = "Maximum value of the input range to be used for the conversion (default is max type value for integer types, or 1 for real types).")
		private Double maxValue;

		@Option(name = "-f", aliases = { "--force" }, required = false, usage = "Will overwrite existing output dataset if specified.")
		private Boolean force;

		private int[] blockSize;
		private boolean parsedSuccessfully = false;

		public Arguments( final String... args )
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );

				blockSize = blockSizeStr != null ? CmdUtils.parseIntArray( blockSizeStr ) : null;

				if ( Objects.isNull( minValue ) != Objects.isNull( maxValue ) )
					throw new IllegalArgumentException( "minValue and maxValue should be either both specified or omitted." );

				this.force = Optional.ofNullable( this.force ).orElse( false );

				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public boolean parsedSuccessfully() { return parsedSuccessfully; }
		public String getInputN5Path() { return n5InputPath; }
		public String getOutputN5Path() { return n5OutputPath != null ? n5OutputPath : n5InputPath; }
		public String getInputDatasetPath() { return inputDatasetPath; }
		public String getOutputDatasetPath() { return outputDatasetPath; }
		public int[] getBlockSize() { return blockSize; }
		public Compression getCompression() { return n5Compression != null ? n5Compression.get() : null; }
		public DataType getDataType() { return dataType; }
		public Pair< Double, Double > getValueRange() { return Objects.nonNull( minValue ) && Objects.nonNull( maxValue ) ? new ValuePair<>( minValue, maxValue ) : null; }
	}

	private static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final long[] gridOffset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition) {

		for (int d = 0; d < max.length; ++d) {
			croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
			intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
			gridPosition[d] = offset[d] / blockDimensions[d] + gridOffset[d];
		}
	}

	private static final void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final long[] gridOffset) throws IOException {

		final Interval zeroMinInterval = new FinalInterval(Intervals.dimensionsAsLongArray(interval));
		final int n = zeroMinInterval.numDimensions();
		final long[] max = Intervals.maxAsLongArray(zeroMinInterval);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			n5.deleteBlock(dataset, gridPosition);
			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}
}
