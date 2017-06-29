package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.util.Intervals;

public class N5DownsamplingSparkTest
{
	static private final String basePath = System.getProperty("user.home") + "/tmp/n5-downsampling-test";
	static private final String datasetPath = "data";

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "N5DownsamplingTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);
	}

	@After
	public void tearDown() throws IOException
	{
		if ( sparkContext != null )
			sparkContext.close();

		if ( Files.exists( Paths.get( basePath ) ) )
			cleanup( N5.openFSWriter( basePath ) );
	}

	private void createDataset( final N5Writer n5 ) throws IOException
	{
		final long[] dimensions = new long[] { 4, 4, 4 };
		final int[] cellSize = new int[] { 1, 1, 1 };

		final byte[] data = new byte[ ( int ) Intervals.numElements( dimensions ) ];
		for ( byte i = 0; i < data.length; ++i )
			data[ i ] = i;

		N5Utils.save( ArrayImgs.bytes( data, dimensions ), n5, datasetPath, cellSize, CompressionType.GZIP );
	}

	private void cleanup( final N5Writer n5 ) throws IOException
	{
		n5.remove( "" );
	}

	@Test
	public void test() throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		createDataset( n5 );

		final int[][] scales = N5DownsamplingSpark.downsample( sparkContext, basePath, datasetPath );

		Assert.assertTrue( scales.length == 2 );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scales[ 0 ] );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, scales[ 1 ] );

		final String downsampledDatasetPath = Paths.get( "s1" ).toString();

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles().length == 2 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 2 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, downsampledAttributes.getBlockSize() );

		for ( final byte zCoord : new byte[] { 0, 1 } )
		{
			final byte zOffset = ( byte ) ( zCoord * 32 );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 0  + 1  + 4  + 5  + 16 + 17 + 20 + 21 ) / 8. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 2  + 3  + 6  + 7  + 18 + 19 + 22 + 23 ) / 8. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 8  + 9  + 12 + 13 + 24 + 25 + 28 + 29 ) / 8. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 1, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 10 + 11 + 14 + 15 + 26 + 27 + 30 + 31 ) / 8. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 1, zCoord } ).getData() );
		}

		cleanup( n5 );
	}

	@Test
	public void testIsotropic() throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		createDataset( n5 );

		final VoxelDimensions voxelSize = new FinalVoxelDimensions( "um", 0.1, 0.1, 0.2 );
		final int[][] scales = N5DownsamplingSpark.downsampleIsotropic( sparkContext, basePath, datasetPath, voxelSize );

		Assert.assertTrue( scales.length == 2 );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scales[ 0 ] );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scales[ 1 ] );

		final String downsampledDatasetPath = Paths.get( "s1" ).toString();

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles().length == 2 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 4 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, downsampledAttributes.getBlockSize() );

		for ( final byte zCoord : new byte[] { 0, 1 } )
		{
			final byte zOffset = ( byte ) ( zCoord * 32 );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 0  + 1  + 4  + 5  ) / 4. ), ( byte ) Math.round( zOffset + ( 16 + 17 + 20 + 21 ) / 4. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 2  + 3  + 6  + 7  ) / 4. ), ( byte ) Math.round( zOffset + ( 18 + 19 + 22 + 23 ) / 4. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 0, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 8  + 9  + 12 + 13 ) / 4. ), ( byte ) Math.round( zOffset + ( 24 + 25 + 28 + 29 ) / 4. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 1, zCoord } ).getData() );
			Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( zOffset + ( 10 + 11 + 14 + 15 ) / 4. ), ( byte ) Math.round( zOffset + ( 26 + 27 + 30 + 31 ) / 4. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 1, zCoord } ).getData() );
		}

		cleanup( n5 );
	}
}
