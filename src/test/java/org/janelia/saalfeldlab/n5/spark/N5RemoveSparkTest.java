package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class N5RemoveSparkTest
{
	static private String basePath = System.getProperty("user.home") + "/tmp/n5-test";
	static private String groupName = "/test/group";
	static private String datasetName = "/test/group/dataset";

	static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
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
			cleanup( n5Supplier.get() );
	}

	private void cleanup( final N5Writer n5 ) throws IOException
	{
		n5.remove( "" );
	}

	@Test
	public void test() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();

		final short[] data = new short[64 * 64 * 64];
		final Random rnd = new Random();
		for (int i = 0; i < data.length; ++i)
			data[ i ] = ( short ) ( rnd.nextInt() % ( Short.MAX_VALUE - Short.MIN_VALUE + 1 ) );

		final int nBlocks = 5;
		n5.createDataset( datasetName, new long[]{ 64 * nBlocks, 64 * nBlocks, 64 * nBlocks }, new int[]{ 64, 64, 64 }, DataType.UINT16, new RawCompression() );
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetName );
		for (int z = 0; z < nBlocks; ++z)
			for (int y = 0; y < nBlocks; ++y)
				for (int x = 0; x < nBlocks; ++x) {
					final ShortArrayDataBlock dataBlock = new ShortArrayDataBlock(new int[]{64, 64, 64}, new long[]{x, y, z}, data);
					n5.writeBlock(datasetName, attributes, dataBlock);
				}

		N5RemoveSpark.remove( sparkContext, n5Supplier, datasetName );
		Assert.assertFalse( Files.exists( Paths.get( basePath, datasetName ) ) );
		Assert.assertTrue( Files.exists( Paths.get( basePath, groupName ) ) );

		N5RemoveSpark.remove( sparkContext, n5Supplier, "" );
		Assert.assertFalse( Files.exists( Paths.get( basePath ) ) );
	}
}
