package org.janelia.saalfeldlab.n5.spark.downsample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
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

public class N5OffsetDownsamplerSparkTest
{
	static private final String basePath = System.getProperty("user.home") + "/.n5-spark-test-" + RandomStringUtils.randomAlphanumeric(5);
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
				.setAppName( "N5OffsetDownsamplerTest" )
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
	public void testDownsamplingWithPositiveOffset1D() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		for ( int blockSize = 1; blockSize <= 10; ++blockSize )
		{
			createDataset( n5, new long[] { 9 }, new int[] { blockSize } );

			N5OffsetDownsamplerSpark.downsampleWithOffset(
					sparkContext,
					n5Supplier,
					datasetPath,
					downsampledDatasetPath,
					new int[] { 4 },
					new long[] { 3 }
				);

			final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
			Assert.assertArrayEquals( new long[] { 3 }, downsampledAttributes.getDimensions() );
			Assert.assertArrayEquals( new int[] { blockSize }, downsampledAttributes.getBlockSize() );

			Assert.assertArrayEquals(
					new int[] {
							1,
							( int ) Util.round( ( 2 + 3 + 4 + 5 ) / 4. ),
							( int ) Util.round( ( 6 + 7 + 8 + 9 ) / 4. )
						},
					getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
				);

			Assert.assertTrue( n5Supplier.get().remove( downsampledDatasetPath ) );
		}
	}

	@Test
	public void testDownsamplingWithNegativeOffset1D() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		for ( int blockSize = 1; blockSize <= 10; ++blockSize )
		{
			createDataset( n5, new long[] { 10 }, new int[] { blockSize } );

			N5OffsetDownsamplerSpark.downsampleWithOffset(
					sparkContext,
					n5Supplier,
					datasetPath,
					downsampledDatasetPath,
					new int[] { 3 },
					new long[] { -1 }
				);

			final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
			Assert.assertArrayEquals( new long[] { 3 }, downsampledAttributes.getDimensions() );
			Assert.assertArrayEquals( new int[] { blockSize }, downsampledAttributes.getBlockSize() );

			Assert.assertArrayEquals(
					new int[] {
							( int ) Util.round( ( 2 + 3 + 4  ) / 3. ),
							( int ) Util.round( ( 5 + 6 + 7  ) / 3. ),
							( int ) Util.round( ( 8 + 9 + 10 ) / 3. )
						},
					getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
				);

			Assert.assertTrue( n5Supplier.get().remove( downsampledDatasetPath ) );
		}
	}

	@Test
	public void testDownsamplingWithOffset2D() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		for ( int blockSizeX = 1; blockSizeX <= 6; ++blockSizeX )
		{
			for ( int blockSizeY = 1; blockSizeY <= 8; ++blockSizeY )
			{
				createDataset( n5, new long[] { 5, 7 }, new int[] { blockSizeX, blockSizeY } );

				N5OffsetDownsamplerSpark.downsampleWithOffset(
						sparkContext,
						n5Supplier,
						datasetPath,
						downsampledDatasetPath,
						new int[] { 3, 3 },
						new long[] { 2, -2 }
					);

				final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
				Assert.assertArrayEquals( new long[] { 2, 1 }, downsampledAttributes.getDimensions() );
				Assert.assertArrayEquals( new int[] { blockSizeX, blockSizeY }, downsampledAttributes.getBlockSize() );

				Assert.assertArrayEquals(
						new int[] {
								( int ) Util.round( ( 11 + 16 + 21 ) / 3. ),
								( int ) Util.round( ( 12 + 13 + 14 + 17 + 18 + 19 + 22 + 23 + 24 ) / 9. ),
							},
						getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
					);

				Assert.assertTrue( n5Supplier.get().remove( downsampledDatasetPath ) );
			}
		}
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
