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

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark.ClampingConverter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class N5ConvertSparkTest extends AbstractN5SparkTest
{
	static private final String datasetPath = "data";
	static private final String convertedDatasetPath = "converted-data";

	@Test
	public void testTypeConversion()
	{
		final ClampingConverter< DoubleType, UnsignedIntType > converter = new ClampingConverter<>( -1.0, 1.0, 0, 4 );
		Assert.assertEquals( 0, getConvertedValue( converter, -1.0 ) );
		Assert.assertEquals( 1, getConvertedValue( converter, -0.75 ) );
		Assert.assertEquals( 1, getConvertedValue( converter, -0.5 ) );
		Assert.assertEquals( 2, getConvertedValue( converter, -0.25 ) );
		Assert.assertEquals( 2, getConvertedValue( converter, 0.0 ) );
		Assert.assertEquals( 3, getConvertedValue( converter, 0.25 ) );
		Assert.assertEquals( 3, getConvertedValue( converter, 0.5 ) );
		Assert.assertEquals( 4, getConvertedValue( converter, 0.75 ) );
		Assert.assertEquals( 4, getConvertedValue( converter, 1.0 ) );
	}

	@Test
	public void testShortToByte() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		final long[] dimensions = new long[] { 4, 5, 6 };
		N5Utils.save( createImage( new ShortType( ( short ) 50 ), dimensions ), n5, datasetPath, new int[] { 2, 3, 1 }, new XzCompression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				() -> new N5FSWriter( basePath ),
				convertedDatasetPath,
				Optional.of( new int[] { 5, 1, 2 } ),
				Optional.of( new Bzip2Compression() ),
				Optional.of( DataType.INT8 ),
				Optional.of( new ValuePair<>( new Double( -100 ), new Double( 70 ) ) )
			);

		Assert.assertTrue( n5.datasetExists( convertedDatasetPath ) );

		final DatasetAttributes convertedAttributes = n5.getDatasetAttributes( convertedDatasetPath );
		Assert.assertArrayEquals( dimensions, convertedAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 5, 1, 2 }, convertedAttributes.getBlockSize() );
		Assert.assertEquals( new Bzip2Compression().getType(), convertedAttributes.getCompression().getType() );
		Assert.assertEquals( DataType.INT8, convertedAttributes.getDataType() );

		Assert.assertArrayEquals(
				( int[] ) ( ( ArrayDataAccess< ? > ) createImage( new IntType( 97 ), dimensions ).update( null ) ).getCurrentStorageArray(),
				( int[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5, convertedDatasetPath ), new IntType() ).update( null ) ).getCurrentStorageArray()
			);
	}

	@Test
	public void testFloatToUnsignedByte() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		final long[] dimensions = new long[] { 4, 5, 6 };
		N5Utils.save( createImage( new FloatType( ( float ) 0.25 ), dimensions ), n5, datasetPath, new int[] { 2, 3, 1 }, new RawCompression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				() -> new N5FSWriter( basePath ),
				convertedDatasetPath,
				Optional.of( new int[] { 5, 1, 2 } ),
				Optional.of( new GzipCompression() ),
				Optional.of( DataType.UINT8 ),
				Optional.empty()
			);

		Assert.assertTrue( n5.datasetExists( convertedDatasetPath ) );

		final DatasetAttributes convertedAttributes = n5.getDatasetAttributes( convertedDatasetPath );
		Assert.assertArrayEquals( dimensions, convertedAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 5, 1, 2 }, convertedAttributes.getBlockSize() );
		Assert.assertEquals( new GzipCompression().getType(), convertedAttributes.getCompression().getType() );
		Assert.assertEquals( DataType.UINT8, convertedAttributes.getDataType() );

		Assert.assertArrayEquals(
				( int[] ) ( ( ArrayDataAccess< ? > ) createImage( new IntType( 64 ), dimensions ).update( null ) ).getCurrentStorageArray(),
				( int[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5, convertedDatasetPath ), new IntType() ).update( null ) ).getCurrentStorageArray()
			);
	}

	@Test
	public void testAdjustedInputBlocks() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		final long[] dimensions = new long[] { 30, 30, 30 };

		final short[] inputData = new short[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < inputData.length; ++i )
			inputData[ i ] = ( short ) ( i + 1 );

		N5Utils.save( ArrayImgs.shorts( inputData, dimensions ), n5, datasetPath, new int[] { 6, 7, 8 }, new Lz4Compression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				() -> new N5FSWriter( basePath ),
				convertedDatasetPath,
				Optional.of( new int[] { 5, 3, 3 } ),
				Optional.empty(),
				Optional.empty(),
				Optional.empty()
			);

		Assert.assertTrue( n5.datasetExists( convertedDatasetPath ) );

		final DatasetAttributes convertedAttributes = n5.getDatasetAttributes( convertedDatasetPath );
		Assert.assertArrayEquals( dimensions, convertedAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 5, 3, 3 }, convertedAttributes.getBlockSize() );
		Assert.assertEquals( new Lz4Compression().getType(), convertedAttributes.getCompression().getType() );
		Assert.assertEquals( DataType.INT16, convertedAttributes.getDataType() );

		Assert.assertArrayEquals(
				inputData,
				( short[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5, convertedDatasetPath ), new ShortType() ).update( null ) ).getCurrentStorageArray()
			);
	}

	private < T extends NativeType< T > & RealType< T > > ArrayImg< T, ? > createImage( final T value, final long... dimensions )
	{
		final ArrayImg< T, ? > img = new ArrayImgFactory<>( value.createVariable() ).create( dimensions );
		final Cursor< T > imgCursor = img.cursor();
		while ( imgCursor.hasNext() )
			imgCursor.next().set( value );
		return img;
	}

	private < T extends NativeType< T > & RealType< T > > ArrayImg< T, ? > getImgFromRandomAccessibleInterval( final RandomAccessibleInterval< ? extends RealType< ? > > rai, final T value )
	{
		final ArrayImg< T, ? > img = new ArrayImgFactory<>( value.createVariable() ).create( Intervals.dimensionsAsLongArray( rai ) );
		final Cursor< ? extends RealType< ? > > raiCursor = Views.flatIterable( rai ).cursor();
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();
		while ( raiCursor.hasNext() || imgCursor.hasNext() )
			imgCursor.next().setReal( raiCursor.next().getRealDouble() );
		return img;
	}

	private int getConvertedValue( final ClampingConverter< DoubleType, UnsignedIntType > converter, final double value )
	{
		final UnsignedIntType convertedValue = new UnsignedIntType();
		converter.convert( new DoubleType( value ), convertedValue );
		return convertedValue.getInt();
	}
}
