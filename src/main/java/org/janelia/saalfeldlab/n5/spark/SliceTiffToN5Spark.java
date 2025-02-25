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
package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.imglib2.algorithm.util.Singleton;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
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
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import ij.ImagePlus;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Grids;
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
	 * @throws IOException
	 */
	public static void convert(
			final JavaSparkContext sparkContext,
			final String inputDirPath,
			final N5WriterSupplier outputN5Supplier,
			final String outputDataset,
			final int[] blockSize,
			final Compression compression ) throws IOException
	{
		convert(
				sparkContext,
				inputDirPath,
				new TiffUtils.FileTiffReader(),
				outputN5Supplier,
				outputDataset,
				blockSize,
				compression );
	}

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
	 * @throws IOException
	 */
	@SuppressWarnings( "unchecked" )
	public static < T extends NativeType< T > > void convert(
			final JavaSparkContext sparkContext,
			final String inputDirPath,
			final TiffUtils.TiffReader tiffReader,
			final N5WriterSupplier outputN5Supplier,
			final String outputDataset,
			final int[] blockSize,
			final Compression compression ) throws IOException
	{
		if ( blockSize.length != 3 )
			throw new IllegalArgumentException( "Expected 3D block size." );

		// TODO: Support cloud backends as well. Some of the useful functionality is already implemented in
		//  DataProvider class hierarchy in stitching-spark. Consider moving it n5-spark.
		if ( !( tiffReader instanceof TiffUtils.FileTiffReader ) )
			throw new UnsupportedOperationException( "Backends other than the filesystem are not supported yet" );

		// list tiff slice files in natural order
		final List< String > tiffSliceFilepaths = Files.walk( Paths.get( inputDirPath ) )
			.filter( p -> p.toString().toLowerCase().endsWith( ".tif" ) || p.toString().toLowerCase().endsWith( ".tiff" ) )
			.map( p -> p.toString() )
			.sorted( new AlphanumericComparator( Collator.getInstance() ) )
			.collect( Collectors.toList() );

		if ( tiffSliceFilepaths.isEmpty() )
			throw new RuntimeException( "Specified input directory does not contain any TIFF slices" );

		// open the first image in the series to find out the size of the dataset and its data type
		final long[] dimensions;
		final DataType dataType;
		{
			final ImagePlus imp = tiffReader.openTiff( tiffSliceFilepaths.iterator().next() );
			final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
			if ( img.numDimensions() != 2 )
				throw new RuntimeException( "TIFF images in the specified directory are not 2D" );

			dimensions = new long[] { img.dimension( 0 ), img.dimension( 1 ), tiffSliceFilepaths.size() };
			dataType = N5Utils.dataType( Util.getTypeFromInterval( img ) );
		}

		final N5Writer n5 = outputN5Supplier.get();
		if ( n5.datasetExists( outputDataset ) )
			throw new RuntimeException( "Output N5 dataset already exists." );

		final String tmpDataset = outputDataset + "-tmp";
		if ( n5.datasetExists( tmpDataset ) )
			throw new RuntimeException( "Temporary N5 dataset (" + tmpDataset + ") already exists, please delete it and run again." );

		final int[] tmpBlockSize = new int[ 3 ];
		tmpBlockSize[ 2 ] = 1;
		for ( int d = 0; d < 2; ++d )
			tmpBlockSize[ d ] = blockSize[ d ] * Math.max( ( int ) Math.round( Math.sqrt( blockSize[ 2 ] ) ), 1 );

		// convert to temporary N5 dataset with block size = 1 in the slice dimension and increased block size in other dimensions
		n5.createDataset( tmpDataset, dimensions, tmpBlockSize, dataType, compression );
		final List< Integer > sliceIndices = IntStream.range( 0, tiffSliceFilepaths.size() ).boxed().collect( Collectors.toList() );
		sparkContext.parallelize( sliceIndices, Math.min( sliceIndices.size(), MAX_PARTITIONS ) ).foreach( sliceIndex ->
			{
				final ImagePlus imp = tiffReader.openTiff( tiffSliceFilepaths.get( sliceIndex ) );
				final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
				final String tmpWriterCacheKey = new URIBuilder(outputN5Supplier.get().getURI())
						.setParameters(
								new BasicNameValuePair("type", "writer"),
								new BasicNameValuePair("dataset", tmpDataset),
								new BasicNameValuePair("call", "slice-tiff-to-n5-spark")
						).toString();

				final N5Writer tmpWriter = Singleton.get(tmpWriterCacheKey, () -> outputN5Supplier.get());

				N5Utils.saveNonEmptyBlock(
						Views.addDimension( img, 0, 0 ),
						tmpWriter,
						tmpDataset,
						new long[] { 0, 0, sliceIndex },
						Util.getTypeFromInterval( img ).createVariable()
					);
			}
		);
		Singleton.clear();

		// resave the temporary dataset using the requested block size
		final int[] processingBlockSize = { tmpBlockSize[ 0 ], tmpBlockSize[ 1 ], blockSize[ 2 ] }; // minimize number of reads of each temporary block
		n5.createDataset( outputDataset, dimensions, blockSize, dataType, compression );
		final List< Tuple2< long[], long[] > > minMaxTuples = N5SparkUtils.toMinMaxTuples( Grids.collectAllContainedIntervals( dimensions, processingBlockSize ) );
		sparkContext.parallelize( minMaxTuples, Math.min( minMaxTuples.size(), MAX_PARTITIONS ) ).foreach( minMaxTuple ->
			{
				final N5Writer n5Local = outputN5Supplier.get();
				final String tmpReaderCacheKey = new URIBuilder(outputN5Supplier.get().getURI())
						.setParameters(
								new BasicNameValuePair("type", "reader"),
								new BasicNameValuePair("dataset", tmpDataset),
								new BasicNameValuePair("call", "slice-tiff-to-n5-spark")
						).toString();

				final RandomAccessibleInterval< T > tmpImg = Singleton.get(tmpReaderCacheKey, () -> N5Utils.open( n5Local, tmpDataset ));

				final Interval interval = N5SparkUtils.toInterval( minMaxTuple );
				final RandomAccessibleInterval< T > tmpImgCrop = Views.offsetInterval( tmpImg, interval );


				final String tmpWriterCacheKey = new URIBuilder(outputN5Supplier.get().getURI())
						.setParameters(
								new BasicNameValuePair("type", "writer"),
								new BasicNameValuePair("dataset", outputDataset),
								new BasicNameValuePair("call", "slice-tiff-to-n5-spark")
						).toString();

				final N5Writer n5Writer = Singleton.get(tmpWriterCacheKey, () -> n5Local);


				final CellGrid cellGrid = new CellGrid( dimensions, blockSize );
				final long[] gridOffset = new long[ 3 ];
				cellGrid.getCellPosition( Intervals.minAsLongArray( interval ), gridOffset );
				N5Utils.saveNonEmptyBlock( tmpImgCrop, n5Writer, outputDataset, gridOffset, Util.getTypeFromInterval( tmpImgCrop ) );
			}
		);
		Singleton.clear();

		// cleanup the temporary dataset
		N5RemoveSpark.remove( sparkContext, outputN5Supplier, tmpDataset );
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
					parsedArgs.getCompression()
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
	}
}
