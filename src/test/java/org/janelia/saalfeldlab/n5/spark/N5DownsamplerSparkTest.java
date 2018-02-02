package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.img.array.ArrayImgs;
import net.imglib2.util.Intervals;

public class N5DownsamplerSparkTest
{
	static private final String basePath = System.getProperty( "user.home" ) + "/tmp/n5-downsampler-test";
	static private final String datasetPath = "data";
	static private final String downsampledDatasetPath = "downsampled-data";

	static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "N5DownsamplerTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);
	}

	@After
	public void tearDown() throws IOException
	{
		if ( sparkContext != null )
			sparkContext.close();

		if ( Files.exists( Paths.get( basePath ) ) )
			cleanup( n5Supplier.get() );
	}

	private void createDataset( final N5Writer n5, final long[] dimensions, final int[] blockSize ) throws IOException
	{
		final int[] data = new int[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < data.length; ++i )
			data[ i ] = i;
		N5Utils.save( ArrayImgs.ints( data, dimensions ), n5, datasetPath, blockSize, new GzipCompression() );
	}

	private void cleanup( final N5Writer n5 ) throws IOException
	{
		n5.remove();
	}

	@Test
	public void testDownsamplingXYZ() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 1, 1, 1 } );

		N5DownsamplerSpark.downsample(
				sparkContext,
				n5Supplier,
				datasetPath,
				downsampledDatasetPath,
				new int[] { 2, 2, 2 }
			);

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 2 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, downsampledAttributes.getBlockSize() );

		for ( final byte zCoord : new byte[] { 0, 1 } )
		{
			final byte zOffset = ( byte ) ( zCoord * 32 );
			Assert.assertArrayEquals( new int[] { ( int ) Math.round( zOffset + ( 0  + 1  + 4  + 5  + 16 + 17 + 20 + 21 ) / 8. ) }, ( int[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new int[] { ( int ) Math.round( zOffset + ( 2  + 3  + 6  + 7  + 18 + 19 + 22 + 23 ) / 8. ) }, ( int[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new int[] { ( int ) Math.round( zOffset + ( 8  + 9  + 12 + 13 + 24 + 25 + 28 + 29 ) / 8. ) }, ( int[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 1, zCoord } ).getData() );
			Assert.assertArrayEquals( new int[] { ( int ) Math.round( zOffset + ( 10 + 11 + 14 + 15 + 26 + 27 + 30 + 31 ) / 8. ) }, ( int[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 1, zCoord } ).getData() );
		}

		cleanup( n5 );
	}

	@Test
	public void testDownsamplingND() throws IOException
	{
		final Random rnd = new Random();
		final int dim = rnd.nextInt( 7 ) + 1;
		final long[] dimensions = new long[ dim ];
		final int[] blockSize = new int[ dim ];
		for ( int d = 0; d < dim; ++d )
		{
			dimensions[ d ] = rnd.nextInt( 10 ) + 1;
			blockSize[ d ] = rnd.nextInt( 10 ) + 1;
		}

		System.out.format( "Testing downsampler on dataset with dimensions=%s and blockSize=%s", Arrays.toString( dimensions ), Arrays.toString( blockSize ) );

		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, dimensions, blockSize );

		final int[] downsamplingFactors = new int[ dim ];
		for ( int d = 0; d < dim; ++d )
			downsamplingFactors[ d ] = ( int ) dimensions[ d ];

		N5DownsamplerSpark.downsample(
				sparkContext,
				n5Supplier,
				datasetPath,
				downsampledDatasetPath,
				downsamplingFactors
			);

		final long[] expectedDownsampledDimensions = new long[ dim ];
		Arrays.fill( expectedDownsampledDimensions, 1 );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( expectedDownsampledDimensions, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( blockSize, downsampledAttributes.getBlockSize() );

		final long numElements = Intervals.numElements( dimensions );
		Assert.assertArrayEquals( new int[] { ( int ) Math.round( ( ( numElements - 1 ) * numElements / 2 ) / ( double ) numElements ) }, ( int[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[ dim ] ).getData() );

		cleanup( n5 );
	}
}
