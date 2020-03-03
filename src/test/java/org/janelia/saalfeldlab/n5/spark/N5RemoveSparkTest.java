package org.janelia.saalfeldlab.n5.spark;

import org.janelia.saalfeldlab.n5.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class N5RemoveSparkTest extends AbstractN5SparkTest
{
	static private final String groupName = "/test/group";
	static private final String datasetName = "/test/group/dataset";

	@Test
	public void test() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );

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

		N5RemoveSpark.remove( sparkContext, () -> new N5FSWriter( basePath ), datasetName );
		Assert.assertFalse( Files.exists( Paths.get( basePath, datasetName ) ) );
		Assert.assertTrue( Files.exists( Paths.get( basePath, groupName ) ) );

		N5RemoveSpark.remove( sparkContext, () -> new N5FSWriter( basePath ) );
		Assert.assertFalse( Files.exists( Paths.get( basePath ) ) );
	}
}
