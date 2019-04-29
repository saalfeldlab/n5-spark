package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5Compression;
import org.janelia.saalfeldlab.n5.spark.util.N5SparkUtils;
import org.janelia.saalfeldlab.n5.spark.util.SliceDimension;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Grids;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;
import se.sawano.java.text.AlphanumericComparator;

public class SliceTiffToN5Spark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Converts slice TIFF series into an N5 dataset.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param inputDirPath
	 * 			Path to the input directory containing TIFF slices
	 * @param outputN5Supplier
	 * 			{@link N5Writer} supplier
	 * @param outputDataset
	 * 			Output N5 dataset
	 * @param blockSize
	 * 			Output N5 block size
	 * @param compression
	 * 			Output N5 compression
	 * @param sliceDimension
	 * 			Slice dimension of the input images
	 * @throws IOException
	 */
	@SuppressWarnings( "unchecked" )
	public static < T extends NativeType< T > > void convert(
			final JavaSparkContext sparkContext,
			final String inputDirPath,
			final N5WriterSupplier outputN5Supplier,
			final String outputDataset,
			final int[] blockSize,
			final Compression compression,
			final SliceDimension sliceDimension ) throws IOException
	{
		if ( blockSize.length != 3 )
			throw new IllegalArgumentException( "Expected 3D block size." );

		// list tiff slice files in natural order
		final List< String > tiffSliceFilepaths = Files.walk( Paths.get( inputDirPath ) )
			.filter( p -> p.toString().toLowerCase().endsWith( ".tif" ) || p.toString().toLowerCase().endsWith( ".tiff" ) )
			.map( p -> p.toString() )
			.sorted( new AlphanumericComparator( Collator.getInstance() ) )
			.collect( Collectors.toList() );

		if ( tiffSliceFilepaths.isEmpty() )
			throw new RuntimeException( "Specified input directory does not contain any TIFF slices" );

		final int[] sliceDimensionMap = new int[ 2 ];
		for ( int i = 0, d = 0; d < 3; ++d )
			if ( d != sliceDimension.asInteger() )
				sliceDimensionMap[ i++ ] = d;

		// open the first image in the series to find out the size of the dataset and its data type
		final long[] dimensions;
		final DataType dataType;
		{
			final ImagePlus imp = TiffUtils.openTiff( tiffSliceFilepaths.iterator().next() );
			final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
			if ( img.numDimensions() != 2 )
				throw new RuntimeException( "TIFF images in the specified directory are not 2D" );

			dimensions = new long[ 3 ];
			dimensions[ sliceDimensionMap[ 0 ] ] = img.dimension( 0 );
			dimensions[ sliceDimensionMap[ 1 ] ] = img.dimension( 1 );
			dimensions[ sliceDimension.asInteger() ] = tiffSliceFilepaths.size();

			dataType = N5Utils.dataType( Util.getTypeFromInterval( img ) );
		}

		final N5Writer n5 = outputN5Supplier.get();
		if ( n5.datasetExists( outputDataset ) )
			throw new RuntimeException( "Output N5 dataset already exists." );
		n5.createDataset( outputDataset, dimensions, blockSize, dataType, compression );

		final List< Tuple2< long[], long[] > > parallelizeBlocks = N5SparkUtils.toMinMaxTuples( Grids.collectAllContainedIntervals( dimensions, blockSize ) );

		sparkContext.parallelize( parallelizeBlocks, Math.min( parallelizeBlocks.size(), MAX_PARTITIONS ) ).foreach( minMaxTuple ->
			{
				final Interval interval = N5SparkUtils.toInterval( minMaxTuple );

				final T type = N5Utils.forDataType( dataType );
				final RandomAccessibleInterval< T > dstImg = Views.translate(
						new ArrayImgFactory<>( type ).create( interval ),
						Intervals.minAsLongArray( interval )
					);

				final long[] sliceMinMax = {
						interval.min( sliceDimension.asInteger() ),
						interval.max( sliceDimension.asInteger() )
					};

				final Dimensions sliceDimensions = new FinalDimensions(
						dimensions[ sliceDimensionMap[ 0 ] ],
						dimensions[ sliceDimensionMap[ 1 ] ]
					);

				for ( long slice = sliceMinMax[ 0 ]; slice <= sliceMinMax[ 1 ]; ++slice )
				{
					final ImagePlus imp = TiffUtils.openTiff( tiffSliceFilepaths.get( ( int ) slice ) );
					final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
					if ( !Intervals.equalDimensions( img, new FinalInterval( sliceDimensions ) ) )
						throw new RuntimeException( "TIFF images in the specified directory differ in size" );

					final long[] sliceIntervalMin = new long[ 2 ], sliceIntervalMax = new long[ 2 ];
					for ( int d = 0; d < 2; ++d )
					{
						sliceIntervalMin[ d ] = interval.min( sliceDimensionMap[ d ] );
						sliceIntervalMax[ d ] = interval.max( sliceDimensionMap[ d ] );
					}
					final FinalInterval sliceInterval = new FinalInterval( sliceIntervalMin, sliceIntervalMax );

					final RandomAccessibleInterval< T > imgCrop = Views.interval( img, sliceInterval );
					final RandomAccessibleInterval< T > dstImgSlice = Views.hyperSlice( dstImg, sliceDimension.asInteger(), slice );

					final Cursor< T > srcCursor = Views.flatIterable( imgCrop ).cursor();
					final Cursor< T > dstCursor = Views.flatIterable( dstImgSlice ).cursor();
					while ( srcCursor.hasNext() || dstCursor.hasNext() )
						dstCursor.next().set( srcCursor.next() );
				}

				final CellGrid blockGrid = new CellGrid( dimensions, blockSize );
				final long[] gridOffset = new long[ 3 ];
				blockGrid.getCellPosition( Intervals.minAsLongArray( interval ), gridOffset );

				N5Utils.saveNonEmptyBlock(
						dstImg,
						outputN5Supplier.get(),
						outputDataset,
						gridOffset,
						type.createVariable()
					);
			}
		);
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "SliceTiffToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getOutputN5Path() );
			convert(
					sparkContext,
					parsedArgs.getInputDirPath(),
					n5Supplier,
					parsedArgs.getOutputDatasetPath(),
					parsedArgs.getBlockSize(),
					parsedArgs.getCompression(),
					parsedArgs.getSliceDimension()
				);
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -2719585735604464792L;

		@Option(name = "-i", aliases = { "--inputDirPath" }, required = true,
				usage = "Path to an input directory containing TIFF slices")
		private String inputDirPath;

		@Option(name = "-n", aliases = { "--outputN5Path" }, required = true,
				usage = "Path to the output N5 container")
		private String outputN5Path;

		@Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
				usage = "Output dataset path within the N5 container (e.g. data/group/s0)")
		private String outputDatasetPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = true,
				usage = "Block size for the output dataset (three comma-separated values, or single value to be used for all dimensions)")
		private String blockSizeStr;

		@Option(name = "-c", aliases = { "--compression" }, required = false,
				usage = "Compression for the output N5 dataset")
		private N5Compression n5Compression = N5Compression.GZIP;

		@Option(name = "-d", aliases = { "--sliceDimension" }, required = false,
				usage = "Slice dimension as a string")
		private SliceDimension sliceDimension = SliceDimension.Z;

		private int[] blockSize;
		private boolean parsedSuccessfully = false;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );

				blockSize = CmdUtils.parseIntArray( blockSizeStr );
				if ( blockSize.length == 1 )
				{
					final int blockSizeVal = blockSize[ 0 ];
					blockSize = new int[ 3 ];
					Arrays.fill( blockSize, blockSizeVal );
				}
				else if ( blockSize.length != 3 )
				{
					throw new IllegalArgumentException( "Incorrect block size argument specified. Expected three comma-separated values or a single value to be used for all dimensions." );
				}

				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public boolean parsedSuccessfully() { return parsedSuccessfully; }

		public String getInputDirPath() { return inputDirPath; }
		public String getOutputN5Path() { return outputN5Path; }
		public String getOutputDatasetPath() { return outputDatasetPath; }
		public int[] getBlockSize() { return blockSize; }
		public Compression getCompression() { return n5Compression.get(); }
		public SliceDimension getSliceDimension() { return sliceDimension; }
	}
}
