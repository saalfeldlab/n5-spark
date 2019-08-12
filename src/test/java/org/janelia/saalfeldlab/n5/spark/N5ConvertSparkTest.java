package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark.ClampingConverter;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public class N5ConvertSparkTest
{
	static private final String basePath = System.getProperty("user.home") + "/.n5-spark-test-" + RandomStringUtils.randomAlphanumeric(5);
	static private final String datasetPath = "data";
	static private final String convertedDatasetPath = "converted-data";

	static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "N5ConverterTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);
	}

	@After
	public void tearDown() throws IOException
	{
		if ( sparkContext != null )
			sparkContext.close();

		if ( Files.exists( Paths.get( basePath ) ) )
			n5Supplier.get().remove();
	}

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
		final N5Writer n5 = n5Supplier.get();
		final long[] dimensions = new long[] { 4, 5, 6 };
		N5Utils.save( createImage( new ShortType( ( short ) 50 ), dimensions ), n5, datasetPath, new int[] { 2, 3, 1 }, new XzCompression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				n5Supplier,
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
				( int[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5Supplier.get(), convertedDatasetPath ), new IntType() ).update( null ) ).getCurrentStorageArray()
			);
	}

	@Test
	public void testFloatToUnsignedByte() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final long[] dimensions = new long[] { 4, 5, 6 };
		N5Utils.save( createImage( new FloatType( ( float ) 0.25 ), dimensions ), n5, datasetPath, new int[] { 2, 3, 1 }, new RawCompression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				n5Supplier,
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
				( int[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5Supplier.get(), convertedDatasetPath ), new IntType() ).update( null ) ).getCurrentStorageArray()
			);
	}

	@Test
	public void testAdjustedInputBlocks() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final long[] dimensions = new long[] { 30, 30, 30 };

		final short[] inputData = new short[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < inputData.length; ++i )
			inputData[ i ] = ( short ) ( i + 1 );

		N5Utils.save( ArrayImgs.shorts( inputData, dimensions ), n5, datasetPath, new int[] { 6, 7, 8 }, new Lz4Compression() );

		N5ConvertSpark.convert(
				sparkContext,
				() -> new N5FSReader( basePath ),
				datasetPath,
				n5Supplier,
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
				( short[] ) ( ( ArrayDataAccess< ? > ) getImgFromRandomAccessibleInterval( N5Utils.open( n5Supplier.get(), convertedDatasetPath ), new ShortType() ).update( null ) ).getCurrentStorageArray()
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
