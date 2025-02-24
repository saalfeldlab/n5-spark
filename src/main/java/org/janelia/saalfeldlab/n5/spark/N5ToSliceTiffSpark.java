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

import com.esotericsoftware.kryo.Kryo;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Singleton;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg.LazyCells;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.*;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.util.N5SparkUtils;
import org.janelia.saalfeldlab.n5.spark.util.SliceDimension;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils.TiffCompression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class N5ToSliceTiffSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Converts a given dataset into slice TIFF series.
	 * The output images will be named as 0.tif, 1.tif, and so on.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting TIFF series
	 * @param compression
	 * 			TIFF compression to be used for the resulting TIFF series
	 * @param sliceDimension
	 * 			Dimension to slice over
	 * @throws IOException
	 */
	public static void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression,
			final SliceDimension sliceDimension ) throws IOException
	{
		convert(
				sparkContext,
				n5Supplier,
				datasetPath,
				outputPath,
				compression,
				sliceDimension,
				"%d.tif"
			);
	}

	/**
	 * Converts a given dataset into slice TIFF series.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting TIFF series
	 * @param compression
	 * 			TIFF compression to be used for the resulting TIFF series
	 * @param sliceDimension
	 * 			Dimension to slice over
	 * @param filenameFormat
	 * 			Filename format specified using Java string formatter syntax
	 * @throws IOException
	 */
	public static void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression,
			final SliceDimension sliceDimension,
			final String filenameFormat ) throws IOException
	{
		convert(
				sparkContext,
				n5Supplier,
				datasetPath,
				outputPath,
				compression,
				sliceDimension,
				filenameFormat,
				Optional.empty()
		);
	}

	/**
	 * Converts a given dataset into slice TIFF series.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting TIFF series
	 * @param compression
	 * 			TIFF compression to be used for the resulting TIFF series
	 * @param sliceDimension
	 * 			Dimension to slice over
	 * @param filenameFormat
	 * 			Filename format specified using Java string formatter syntax
	 * @param fillValueOptional
	 * 			Intensity value for filling extra space
	 * @throws IOException
	 */
	public static void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression,
			final SliceDimension sliceDimension,
			final String filenameFormat,
			final Optional< Number > fillValueOptional ) throws IOException
	{
		convert(
				sparkContext,
				n5Supplier,
				datasetPath,
				outputPath,
				new TiffUtils.FileTiffWriter(),
				compression,
				sliceDimension,
				filenameFormat,
				fillValueOptional );
	}

	/**
	 * Converts a given dataset into slice TIFF series.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting TIFF series
	 * @param tiffWriter
	 * 			{@link TiffUtils.TiffWriter}
	 * @param compression
	 * 			TIFF compression to be used for the resulting TIFF series
	 * @param sliceDimension
	 * 			Dimension to slice over
	 * @param filenameFormat
	 * 			Filename format specified using Java string formatter syntax
	 * @param fillValueOptional
	 * 			Intensity value for filling extra space
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void convert(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final String outputPath,
			final TiffUtils.TiffWriter tiffWriter,
			final TiffCompression compression,
			final SliceDimension sliceDimension,
			final String filenameFormat,
			final Optional< Number > fillValueOptional ) throws IOException
	{
		final N5Reader n5 = n5Supplier.get();
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = attributes.getDimensions();

		if ( dimensions.length != 3 )
			throw new IllegalArgumentException( "Conversion to slice TIFF series is supported only for 3D datasets" );

		final int[] sliceDimensionMap = new int[ 2 ];
		for ( int i = 0, d = 0; d < 3; ++d )
			if ( d != sliceDimension.asInteger() )
				sliceDimensionMap[ i++ ] = d;
		final long[] sliceDimensions = new long[] { dimensions[ sliceDimensionMap[ 0 ] ], dimensions[ sliceDimensionMap[ 1 ] ] };

		final List< Long > sliceCoords = LongStream.range( 0, dimensions[ sliceDimension.asInteger() ] ).boxed().collect( Collectors.toList() );
		final Number fillValue = fillValueOptional != null && fillValueOptional.isPresent() ? fillValueOptional.get() : null;

		tiffWriter.createDirs( outputPath );

		sparkContext.parallelize( sliceCoords, Math.min( sliceCoords.size(), MAX_PARTITIONS ) ).foreach( slice ->
			{
				final N5Reader n5Local = n5Supplier.get();
				final URI readerURi = n5Local.getURI();
				final String readerCacheKey = new URIBuilder(readerURi).setParameters(
						new BasicNameValuePair("type", "reader"),
						new BasicNameValuePair("group", "output"),
						new BasicNameValuePair("dataset", datasetPath),
						new BasicNameValuePair("call", "create-max-intensity-projection")
				).toString();
				final CachedCellImg<T, ?> cellImg = Singleton.get(readerCacheKey, () -> N5SparkUtils.openWithLoaderCache(n5, datasetPath, new SoftRefLoaderCache<>()));

				final CellGrid cellGrid = cellImg.getCellGrid();
				final long[] slicePos = new long[ cellImg.numDimensions() ], cellPos = new long[ cellImg.numDimensions() ];
				slicePos[ sliceDimension.asInteger() ] = slice;
				cellGrid.getCellPosition( slicePos, cellPos );

				final Pair< U, long[] > mappedTypeAndRange = getMappedTypeAndRange( Util.getTypeFromInterval( cellImg ) );
				final ImagePlusImg< U, ? > target = new ImagePlusImgFactory<>( mappedTypeAndRange.getA() ).create( sliceDimensions );
				final long[] validMinMax = mappedTypeAndRange.getB();

				if ( fillValue != null )
				{
					final double fillValueDouble = fillValue.doubleValue();

					if ( validMinMax != null )
					{
						final long fillValueLong = Math.round( fillValueDouble );
						if ( fillValueLong < validMinMax[ 0 ] || fillValueLong > validMinMax[ 1 ] )
							throw new IllegalArgumentException( "Fill value is outside the allowed range of values that ImagePlus can support" );
					}

					for ( final U val : target )
						val.setReal( fillValueDouble );
				}

				final LazyCells< ? extends Cell< ? > > cells = cellImg.getCells();
				final long[] cellGridMin = new long[ cellImg.numDimensions() ], cellGridMax = new long[ cellImg.numDimensions() ];
				cells.min( cellGridMin );
				cells.max( cellGridMax );
				cellGridMin[ sliceDimension.asInteger() ] = cellGridMax[ sliceDimension.asInteger() ] = cellPos[ sliceDimension.asInteger() ];
				final Interval cellGridInterval = new FinalInterval( cellGridMin, cellGridMax );

				final RandomAccessibleInterval< ? extends Cell< ? > > sliceCells = Views.interval( cells, cellGridInterval );
				final Cursor< ? extends Cell< ? > > sliceCellsCursor = Views.iterable( sliceCells ).cursor();

				final long[] cellMin = new long[ cellImg.numDimensions() ], cellMax = new long[ cellImg.numDimensions() ];
				final int[] cellDimensions = new int[ cellImg.numDimensions() ];

				while ( sliceCellsCursor.hasNext() )
				{
					final Cell< ? > cell = sliceCellsCursor.next();
					cell.min( cellMin );
					cell.dimensions( cellDimensions );
					for ( int d = 0; d < cellImg.numDimensions(); ++d )
						cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
					cellMin[ sliceDimension.asInteger() ] = cellMax[ sliceDimension.asInteger() ] = slice;
					final Interval sourceInterval = new FinalInterval( cellMin, cellMax );

					final Interval targetInterval = new FinalInterval(
							new long[] { cellMin[ sliceDimensionMap[ 0 ] ], cellMin[ sliceDimensionMap[ 1 ] ] },
							new long[] { cellMax[ sliceDimensionMap[ 0 ] ], cellMax[ sliceDimensionMap[ 1 ] ] }
						);

					final Cursor< T > sourceCursor = Views.flatIterable( Views.interval( cellImg, sourceInterval ) ).cursor();

					// check the value range if type conversion is needed
					if ( validMinMax != null )
					{
						while ( sourceCursor.hasNext() )
						{
							final T val = sourceCursor.next();
							final long valLong = Math.round( val.getRealDouble() );
							if ( valLong < validMinMax[ 0 ] || valLong > validMinMax[ 1 ] )
								throw new IllegalArgumentException( "Data value is outside the allowed range of values that ImagePlus can support" );
						}
						sourceCursor.reset();
					}

					final Cursor< U > targetCursor = Views.flatIterable( Views.interval( target, targetInterval ) ).cursor();
					while ( sourceCursor.hasNext() || targetCursor.hasNext() )
						targetCursor.next().setReal( sourceCursor.next().getRealDouble() );
				}

				final ImagePlus sliceImp = target.getImagePlus();
				final String outputImgPath = tiffWriter.combinePaths( outputPath, String.format( filenameFormat, slice ) );
				tiffWriter.saveTiff( sliceImp, outputImgPath, compression );
			}
		);
	}

	/**
	 * ImagePlus supports only three types:
	 * - GRAY8 (unsigned byte)
	 * - GRAY16 (unsigned short)
	 * - GRAY32 (float)
	 *
	 * If the input image is of different type, it needs to be mapped to one of the ImagePlus-supported types.
	 *
	 * @param sourceType
	 * @param <T>
	 * @param <U>
	 * @return
	 */
	private static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > Pair< U, long[] > getMappedTypeAndRange( final T sourceType )
	{
		if ( sourceType instanceof DoubleType )
			return new ValuePair<>( ( U ) new FloatType(), null ); // conversion is possible, potentially with loss of precision
		else if ( sourceType instanceof ByteType )
			return new ValuePair<>( ( U ) new UnsignedByteType(), new long[] { 0, 255 } );
		else if ( sourceType instanceof ShortType || sourceType instanceof IntType || sourceType instanceof UnsignedIntType || sourceType instanceof LongType || sourceType instanceof UnsignedLongType )
			return new ValuePair<>( ( U ) new UnsignedShortType(), new long[] { 0, 65535 } );

		return new ValuePair<>( ( U ) sourceType.createVariable(), null );
	}

	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5ToSliceTiffSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5ReaderSupplier n5Supplier = () -> new N5FSReader( parsedArgs.n5Path );
			convert(
					sparkContext,
					n5Supplier,
					parsedArgs.inputDatasetPath,
					parsedArgs.outputPath,
					parsedArgs.tiffCompression,
					parsedArgs.sliceDimension,
					parsedArgs.filenameFormat,
					Optional.ofNullable( parsedArgs.fillValue )
				);
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -2719585735604464792L;

		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0)")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--outputPath" }, required = true,
				usage = "Output path for storing slice TIFF series")
		private String outputPath;

		@Option(name = "-c", aliases = { "--tiffCompression" }, required = false,
				usage = "Tiff compression (not used by default)."
						+ "WARNING: LZW compressor can be very slow. It is not recommended for general use unless saving disk space is crucial.")
		private TiffCompression tiffCompression = TiffCompression.NONE;

		@Option(name = "-d", aliases = { "--sliceDimension" }, required = false,
				usage = "Dimension to slice over as a string")
		private SliceDimension sliceDimension = SliceDimension.Z;

		@Option(name = "-f", aliases = { "--filenameFormat" }, required = false,
				usage = "Filename format (by default output files are named 1.tif, 2.tif, and so on)")
		private String filenameFormat = "%d.tif";

		@Option(name = "--fill", aliases = { "--fillValue" }, required = false,
				usage = "Intensity value for filling extra space (default is 0)")
		private Double fillValue = null;

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
	}
}
