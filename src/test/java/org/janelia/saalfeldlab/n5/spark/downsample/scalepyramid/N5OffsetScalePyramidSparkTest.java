package org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.AbstractN5SparkTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class N5OffsetScalePyramidSparkTest extends AbstractN5SparkTest
{
	static private final String datasetPath = "data";

	@Test
	public void testDownsampling() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 1, 1, 1 } );

		final List< String > scalePyramidDatasets = N5OffsetScalePyramidSpark.downsampleOffsetScalePyramid(
				sparkContext,
				() -> new N5FSWriter( basePath ),
				datasetPath,
				new int[] { 2, 2, 2 },
				new boolean[] { true, true, true }
			);

		Assert.assertEquals( 2, scalePyramidDatasets.size() );

		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, n5.getAttribute( scalePyramidDatasets.get( 0 ), N5OffsetScalePyramidSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );
		Assert.assertArrayEquals( new int[] { 4, 4, 4 }, n5.getAttribute( scalePyramidDatasets.get( 1 ), N5OffsetScalePyramidSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );

		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, n5.getAttribute( scalePyramidDatasets.get( 0 ), N5OffsetScalePyramidSpark.OFFSETS_ATTRIBUTE_KEY, long[].class ) );
		Assert.assertArrayEquals( new long[] { 2, 2, 2 }, n5.getAttribute( scalePyramidDatasets.get( 1 ), N5OffsetScalePyramidSpark.OFFSETS_ATTRIBUTE_KEY, long[].class ) );

		final String downsampledIntermediateDatasetPath = Paths.get( scalePyramidDatasets.get( 0 ) ).toString();
		final String downsampledLastDatasetPath = Paths.get( scalePyramidDatasets.get( 1 ) ).toString();

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, downsampledAttributes.getBlockSize() );

		Assert.assertArrayEquals( new int[] { ( int ) Util.round( ( 1 + 2 + 5 + 6 + 17 + 18 + 21 + 22 ) / 8. ) }, getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledLastDatasetPath ) ) );
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
