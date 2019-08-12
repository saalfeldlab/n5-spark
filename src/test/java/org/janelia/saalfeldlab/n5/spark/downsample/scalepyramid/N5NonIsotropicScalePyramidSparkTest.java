package org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark.NonIsotropicMetadata3D;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark.NonIsotropicScalePyramidMetadata3D;
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

public class N5NonIsotropicScalePyramidSparkTest
{
	static private final String basePath = System.getProperty("user.home") + "/.n5-spark-test-" + RandomStringUtils.randomAlphanumeric(5);
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
				.setAppName( "N5NonIsotropicScalePyramidTest" )
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

	@Test
	public void testNonIsotropicDownsampling() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 2, 2, 1 } );

		final List< String > downsampledDatasets = N5NonIsotropicScalePyramidSpark.downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				datasetPath,
				new double[] { 0.1, 0.1, 0.2 },
				false
			);

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();
		Assert.assertArrayEquals( new String[] { downsampledIntermediateDatasetPath, downsampledLastDatasetPath }, downsampledDatasets.toArray( new String[ 0 ] ) );

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath )
			);

		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, n5.getAttribute( downsampledIntermediateDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );
		Assert.assertArrayEquals( new int[] { 4, 4, 2 }, n5.getAttribute( downsampledLastDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );

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
	}

	@Test
	public void testNonIsotropicDownsampling_Z() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4 }, new int[] { 1, 1, 2 } );

		final List< String > downsampledDatasets = N5NonIsotropicScalePyramidSpark.downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				datasetPath,
				new double[] { 0.2, 0.2, 0.1 },
				false
			);

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();
		Assert.assertArrayEquals( new String[] { downsampledIntermediateDatasetPath, downsampledLastDatasetPath }, downsampledDatasets.toArray( new String[ 0 ] ) );

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath ) );

		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, n5.getAttribute( downsampledIntermediateDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );
		Assert.assertArrayEquals( new int[] { 2, 2, 4 }, n5.getAttribute( downsampledLastDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, downsampledAttributes.getBlockSize() );

		Assert.assertArrayEquals(
				new int[] {
						( int ) Util.round( ( 1+2+5+6 + 17+18+21+22 + 33+34+37+38 + 49+50+53+54) / 16. ),
						( int ) Util.round( ( 3+4+7+8 + 19+20+23+24 + 35+36+39+40 + 51+52+55+56) / 16. ),
						( int ) Util.round( ( 9+10+13+14 + 25+26+29+30 + 41+42+45+46 + 57+58+61+62) / 16. ),
						( int ) Util.round( ( 11+12+15+16 + 27+28+31+32 + 43+44+47+48 + 59+60+63+64) / 16. ),
				},
				getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledLastDatasetPath ) )
			);
	}

	@Test
	public void testNonIsotropicDownsampling4D() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5, new long[] { 4, 4, 4, 2 }, new int[] { 2, 2, 1, 1 } );

		final List< String > downsampledDatasets = N5NonIsotropicScalePyramidSpark.downsampleNonIsotropicScalePyramid(
				sparkContext,
				n5Supplier,
				datasetPath,
				new double[] { 0.1, 0.1, 0.2 },
				false
			);

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();
		Assert.assertArrayEquals( new String[] { downsampledIntermediateDatasetPath, downsampledLastDatasetPath }, downsampledDatasets.toArray( new String[ 0 ] ) );

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath )
			);

		Assert.assertArrayEquals( new int[] { 2, 2, 1, 1 }, n5.getAttribute( downsampledIntermediateDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );
		Assert.assertArrayEquals( new int[] { 4, 4, 2, 1 }, n5.getAttribute( downsampledLastDatasetPath, N5DownsamplerSpark.DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, int[].class ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 2, 2 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 2, 2, 2, 1 }, downsampledAttributes.getBlockSize() );

		Assert.assertArrayEquals(
				new int[] {
						( int ) Util.round( ( 32 * 33 / 2 ) / 32. ),
						( int ) Util.round( ( 32 * 33 / 2 + 32 * 32 ) / 32. ),

						( int ) Util.round( ( 32 * 33 / 2 + 32 * 64 ) / 32. ),
						( int ) Util.round( ( 32 * 33 / 2 + 32 * 96 ) / 32. )
				},
				getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledLastDatasetPath ) )
			);
	}

	@Test
	public void testScalePyramidMetadata_Isotropic()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 8 }, null, false );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 2 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 64 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 4 }, new double[] { 0.097, 0.097, 0.18 }, false );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 7 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 9 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 3 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 17 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 34 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_PowerOfTwo()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 4 }, new double[] { 0.097, 0.097, 0.18 }, true );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 2 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 32 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_3x()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 3 }, new double[] { 0.5, 0.5, 1.5 }, false );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 3 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 21 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 3 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 12 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 5 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 5 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 11 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 3 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 21 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_3x_PowerOfTwo()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 3 }, new double[] { 0.5, 0.5, 1.5 }, true );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 3 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 6 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 16 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_4x()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 8, 8, 2 }, new double[] { 0.5, 0.5, 2 }, false );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 2 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 32, 32, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 4 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 16, 16, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 8, 8, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 4, 4, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 2, 2, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 1, 1, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 8, 8, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 16 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_8x()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 2048, 2048, 2048 }, new int[] { 256, 256, 32 }, new double[] { 1, 1, 8 }, false );
		Assert.assertEquals( 12, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 2048, 2048, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 32 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 1024, 1024, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 512, 512, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 1024 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 64, 64, 512 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 32, 32, 256 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 16, 16, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 8, 8, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 9 );
		Assert.assertArrayEquals( new long[] { 4, 4, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 512, 512, 64 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 10 );
		Assert.assertArrayEquals( new long[] { 2, 2, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1024, 1024, 128 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 11 );
		Assert.assertArrayEquals( new long[] { 1, 1, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 256 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2048, 2048, 256 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_10x()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 2048, 2048, 2048 }, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 }, false );
		Assert.assertEquals( 12, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 2048, 2048, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 1024, 1024, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 52 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 512, 512, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 104 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 1024 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 64, 64, 682 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 3 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 32, 32, 341 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 6 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 16, 16, 157 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 13 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 8, 8, 78 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 9 );
		Assert.assertArrayEquals( new long[] { 4, 4, 40 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 512, 512, 51 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 10 );
		Assert.assertArrayEquals( new long[] { 2, 2, 20 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1024, 1024, 102 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 11 );
		Assert.assertArrayEquals( new long[] { 1, 1, 9 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2048, 2048, 205 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_10x_PowerOfTwo()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 2048, 2048, 2048 }, new int[] { 256, 256, 26 }, new double[] { 1, 1, 10 }, true );
		Assert.assertEquals( 12, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 2048, 2048, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 1024, 1024, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 52 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 512, 512, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 104 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 1024 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 64, 64, 512 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 32, 32, 256 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 16, 16, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 8, 8, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 9 );
		Assert.assertArrayEquals( new long[] { 4, 4, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 512, 512, 64 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 10 );
		Assert.assertArrayEquals( new long[] { 2, 2, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1024, 1024, 128 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 11 );
		Assert.assertArrayEquals( new long[] { 1, 1, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 256, 256, 208 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2048, 2048, 256 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_10x_BiggerBlock()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 2048, 2048, 2048 }, new int[] { 650, 650, 71 }, new double[] { 1, 1, 10 }, false );
		Assert.assertEquals( 12, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 2048, 2048, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 71 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 1024, 1024, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 142 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 512, 512, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 284 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 1024 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 64, 64, 682 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 3 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 32, 32, 341 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 6 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 16, 16, 157 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 13 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 8, 8, 78 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 9 );
		Assert.assertArrayEquals( new long[] { 4, 4, 40 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 512, 512, 51 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 10 );
		Assert.assertArrayEquals( new long[] { 2, 2, 20 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1024, 1024, 102 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 11 );
		Assert.assertArrayEquals( new long[] { 1, 1, 9 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2048, 2048, 205 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_10x_BiggerBlock_PowerOfTwo()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 2048, 2048, 2048 }, new int[] { 650, 650, 71 }, new double[] { 1, 1, 10 }, true );
		Assert.assertEquals( 12, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 2048, 2048, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 71 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 1024, 1024, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 142 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 512, 512, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 284 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 2048 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 1024 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 64, 64, 512 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 32, 32, 256 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 16, 16, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 8, 8, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 9 );
		Assert.assertArrayEquals( new long[] { 4, 4, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 512, 512, 64 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 10 );
		Assert.assertArrayEquals( new long[] { 2, 2, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1024, 1024, 128 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 11 );
		Assert.assertArrayEquals( new long[] { 1, 1, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 650, 650, 568 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2048, 2048, 256 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_Z()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 3, 3, 8 }, new double[] { 0.15, 0.15, 0.05 }, false );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 3, 3, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 64, 64, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 64, 64, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 21, 21, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 3, 3, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 12, 12, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 5, 5, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 5, 5, 2 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 11, 11, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 3, 3, 1 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 21, 21, 64 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_NonIsotropic_Z_PowerOfTwo()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 64, 64, 64 }, new int[] { 3, 3, 8 }, new double[] { 0.15, 0.15, 0.05 }, true );
		Assert.assertEquals( 7, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 64, 64, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 3, 3, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 64, 64, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 64, 64, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 32, 32, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 16, 16, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 8, 8, 2 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 4, 4, 1 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 6, 6, 8 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 64 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_XY_LargerThanOptimalBlockSize()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 256, 256, 128 }, new int[] { 128, 128, 64 }, new double[] { 1, 1, 10 }, false );
		Assert.assertEquals( 9, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 256, 256, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 128, 128, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 64, 64, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 4, 4, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 32, 32, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 8, 8, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 16, 16, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 16, 16, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 8, 8, 42 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 32, 32, 3 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 4, 4, 21 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 64, 64, 6 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 2, 2, 9 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 128, 128, 13 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 8 );
		Assert.assertArrayEquals( new long[] { 1, 1, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 128 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 256, 256, 26 }, scaleMetadata.downsamplingFactors );
	}

	@Test
	public void testScalePyramidMetadata_Z_LargerThanOptimalBlockSize()
	{
		NonIsotropicMetadata3D scaleMetadata;
		final NonIsotropicScalePyramidMetadata3D scalePyramidMetadata = new NonIsotropicScalePyramidMetadata3D( new long[] { 256, 256, 128 }, new int[] { 128, 128, 64 }, new double[] { 10, 10, 1 }, false );
		Assert.assertEquals( 8, scalePyramidMetadata.getNumScales() );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 0 );
		Assert.assertArrayEquals( new long[] { 256, 256, 128 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 1 );
		Assert.assertArrayEquals( new long[] { 256, 256, 64 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 2 );
		Assert.assertArrayEquals( new long[] { 256, 256, 32 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 4 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 3 );
		Assert.assertArrayEquals( new long[] { 256, 256, 16 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 1, 1, 8 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 4 );
		Assert.assertArrayEquals( new long[] { 128, 128, 8 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 2, 2, 16 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 5 );
		Assert.assertArrayEquals( new long[] { 85, 85, 4 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 3, 3, 32 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 6 );
		Assert.assertArrayEquals( new long[] { 42, 42, 2 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 6, 6, 64 }, scaleMetadata.downsamplingFactors );
		scaleMetadata = scalePyramidMetadata.getScaleMetadata( 7 );
		Assert.assertArrayEquals( new long[] { 19, 19, 1 }, scaleMetadata.dimensions );
		Assert.assertArrayEquals( new int[] { 128, 128, 64 }, scaleMetadata.cellSize );
		Assert.assertArrayEquals( new int[] { 13, 13, 128 }, scaleMetadata.downsamplingFactors );
	}
}
