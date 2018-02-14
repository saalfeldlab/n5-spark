package org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5ScalePyramidSpark;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5ScalePyramidSparkTest
{
	static private final String basePath = System.getProperty( "user.home" ) + "/tmp/n5-downsampler-test";
	static private final String datasetPath = "data";

	static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "N5ScalePyramidTest" )
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
		Assert.assertTrue( n5.remove() );
	}

	@Test
	public void testDownsampling() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 1, 1, 1 } );

		final List< String > downsampledDatasets = N5ScalePyramidSpark.downsampleScalePyramid(
				sparkContext,
				n5Supplier,
				datasetPath,
				new int[] { 2, 2, 2 }
			);

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();
		Assert.assertArrayEquals( new String[] { downsampledIntermediateDatasetPath, downsampledLastDatasetPath }, downsampledDatasets.toArray( new String[ 0 ] ) );

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, downsampledAttributes.getBlockSize() );

		final long numElements = Intervals.numElements( new long[] { 4, 4, 4 } );
		Assert.assertArrayEquals( new int[] { ( int ) Util.round( ( numElements * ( numElements + 1 ) / 2 ) / ( double ) numElements ) }, getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledLastDatasetPath ) ) );

		cleanup( n5 );
	}

	private void createDataset( final N5Writer n5, final long[] dimensions, final int[] blockSize ) throws IOException
	{
		final int[] data = new int[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < data.length; ++i )
			data[ i ] = i + 1;
		N5Utils.save( ArrayImgs.ints( data, dimensions ), n5, datasetPath, blockSize, new GzipCompression() );
	}

	private int[] getArrayFromRandomAccessibleInterval( final RandomAccessibleInterval< IntType > rai )
	{
		final int[] arr = new int[ ( int ) Intervals.numElements( rai ) ];
		final Cursor< IntType > cursor = Views.flatIterable( rai ).cursor();
		int i = 0;
		while ( cursor.hasNext() )
			arr[ i++ ] = cursor.next().get();
		return arr;
	}
}
