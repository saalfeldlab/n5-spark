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
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark3D.IsotropicScalingEstimator;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark3D.IsotropicScalingParameters;
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

public class N5NonIsotropicScalePyramidSpark3DTest
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
				.setAppName( "N5NonIsotropicScalePyramid3DTest" )
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
	public void testIsotropicScalingParameters()
	{
		IsotropicScalingParameters testParams;

		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 0, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 1, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 2, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 4 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 3, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 4, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 16 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 5, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 32 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 6, new int[] { 8, 8, 8 }, null );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 64 }, testParams.downsamplingFactors );

		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 0, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 1, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 2, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 2 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 3, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 4, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 9 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 5, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 17 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 6, new int[] { 8, 8, 8 }, new double[] { 0.097, 0.097, 0.18 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 34 }, testParams.downsamplingFactors );

		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 0, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 3 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 1, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 2, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 9 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 3, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 9 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 3 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 4, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 9 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 5 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 5, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 9 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 11 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 6, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 1.5 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 9 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 21 }, testParams.downsamplingFactors );

		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 0, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 2 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 1, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 2, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 3, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 2 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 4, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 4 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 5, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 8 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 6, new int[] { 8, 8, 8 }, new double[] { 0.5, 0.5, 2 } );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 16 }, testParams.downsamplingFactors );

		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 0, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 1, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 52 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 2, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 104 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 3, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 4, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 5, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 260 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 3 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 6, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 260 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 6 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 7, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 260 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 13 }, testParams.downsamplingFactors );
		testParams = IsotropicScalingEstimator.getOptimalCellSizeAndDownsamplingFactor( 8, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 } );
		Assert.assertArrayEquals( new int[] { 256, 256, 260 }, testParams.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, testParams.downsamplingFactors );
	}

	@Test
	public void testIsotropicDownsampling() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 2, 2, 1 } );

		final List< String > downsampledDatasets = N5NonIsotropicScalePyramidSpark3D.downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				datasetPath,
				new double[] { 0.1, 0.1, 0.2 }
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
		Assert.assertArrayEquals( new long[] { 1, 1, 2 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, downsampledAttributes.getBlockSize() );

		Assert.assertArrayEquals(
				new int[] {
						( int ) Util.round( ( 32 * 33 / 2 ) / 32. ),
						( int ) Util.round( ( 32 * 33 / 2 + 32 * 32) / 32. ),
				},
				getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledLastDatasetPath ) )
			);

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