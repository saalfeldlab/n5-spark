package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.img.array.ArrayImgs;
import net.imglib2.util.Intervals;

public class N5DownsamplingSparkTest
{
	static private final String testDirPath = System.getProperty("user.home") + "/tmp/n5-downsampling-test";

	@Test
	public void testIsotropicDownsampling() throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( testDirPath );
		final long[] dimensions = new long[] { 4, 4, 4 };
		final int[] cellSize = new int[] { 1, 1, 1 };

		final byte[] data = new byte[ ( int ) Intervals.numElements( dimensions ) ];
		for ( byte i = 0; i < data.length; ++i )
			data[ i ] = i;

		final String rootOutputPath = "c0";
		final String fullScaleDatasetPath = Paths.get( rootOutputPath, "s0" ).toString();
		N5Utils.save( ArrayImgs.bytes( data, dimensions ), n5, fullScaleDatasetPath, cellSize, CompressionType.GZIP );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "N5DownsamplingTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			new N5DownsamplingSpark<>( sparkContext ).downsample( testDirPath, fullScaleDatasetPath );

			final String downsampledDatasetPath = Paths.get( rootOutputPath, "s1" ).toString();

			Assert.assertTrue(
					Paths.get( testDirPath, rootOutputPath ).toFile().listFiles().length == 2 &&
					n5.datasetExists( fullScaleDatasetPath ) &&
					n5.datasetExists( downsampledDatasetPath ) );

			final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
			for ( final byte zCoord : new byte[] { 0, 1 } )
			{
				final byte zOffset = ( byte ) ( zCoord * 32 );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 0  + 1  + 4  + 5  ) / 4. ) + Math.round( zOffset + ( 16 + 17 + 20 + 21 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 0, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 2  + 3  + 6  + 7  ) / 4. ) + Math.round( zOffset + ( 18 + 19 + 22 + 23 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 0, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 8  + 9  + 12 + 13 ) / 4. ) + Math.round( zOffset + ( 24 + 25 + 28 + 29 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 0, 1, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 10 + 11 + 14 + 15 ) / 4. ) + Math.round( zOffset + ( 26 + 27 + 30 + 31 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( downsampledDatasetPath, downsampledAttributes, new long[] { 1, 1, zCoord } ).getData() );
			}

			n5.remove( "" );
		}
	}
}
