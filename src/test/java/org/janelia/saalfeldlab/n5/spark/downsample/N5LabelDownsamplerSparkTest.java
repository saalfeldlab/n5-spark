package org.janelia.saalfeldlab.n5.spark.downsample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class N5LabelDownsamplerSparkTest
{
	static private final String basePath = System.getProperty( "user.home" ) + "/tmp/n5-label-downsampler-test";
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
				.setAppName( "N5LabelDownsamplerTest" )
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
	public void testLabelDownsampling() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();

		N5Utils.save(
				ArrayImgs.unsignedLongs(
						new long[] {
								5, 4, 8, 8,
								5, 5, 5, 8,
								5, 6, 6, 6,
								1, 2, 3, 4,

								9, 9, 8, 8,
								7, 5, 8, 8,
								5, 5, 6, 8,
								5, 5, 8, 8
							},
						new long[] { 4, 4, 2 }
					),
				n5,
				datasetPath,
				new int[] { 2, 2, 1 },
				new GzipCompression()
			);

		N5LabelDownsamplerSpark.downsampleLabel(
				sparkContext,
				n5Supplier,
				datasetPath,
				downsampledDatasetPath,
				new int[] { 2, 2, 2 }
			);
		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, downsampledAttributes.getBlockSize() );
		Assert.assertArrayEquals( new long[] { 5, 8, 5, 6 }, getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) ) );

		cleanup( n5 );
	}

	private long[] getArrayFromRandomAccessibleInterval( final RandomAccessibleInterval< UnsignedLongType > rai )
	{
		final long[] arr = new long[ ( int ) Intervals.numElements( rai ) ];
		final Cursor< UnsignedLongType > cursor = Views.flatIterable( rai ).cursor();
		int i = 0;
		while ( cursor.hasNext() )
			arr[ i++ ] = cursor.next().get();
		return arr;
	}
}
