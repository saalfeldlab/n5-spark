package org.janelia.saalfeldlab.n5.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.list.N5SerializableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.list.ListImg;
import net.imglib2.type.Type;
import net.imglib2.type.operators.Add;
import net.imglib2.type.operators.MulFloatingPoint;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class N5DownsamplingSparkSerializableTypeTest
{
	private static class Multiset< K > implements Map< K, Integer >, Type< Multiset< K > >, Add< Multiset< K > >, MulFloatingPoint, Serializable
	{
		private static final long serialVersionUID = 8141475759453243839L;

		private final Map< K, Integer > multiset = new TreeMap<>();
		private long accumulatedCount = 1;

		public long getAccumulatedCount()
		{
			return accumulatedCount;
		}

		@Override
		public Integer get( final Object key )
		{
			return multiset.get( key );
		}

		public Integer put( final K key )
		{
			return put( key, 1 );
		}

		@Override
		public Integer put( final K key, final Integer count )
		{
			final Integer val = multiset.get( key );
			multiset.put( key, ( val != null ? val.intValue() : 0 ) + count );
			return val;
		}

		@Override
		public void putAll( final Map< ? extends K, ? extends Integer > map )
		{
			for ( final Entry< ? extends K, ? extends Integer > entry : map.entrySet() )
				put( entry.getKey(), entry.getValue() );
		}

		@Override
		public int size()
		{
			return multiset.size();
		}

		@Override
		public boolean isEmpty()
		{
			return multiset.isEmpty();
		}

		@Override
		public boolean containsKey( final Object key )
		{
			return multiset.containsKey( key );
		}

		@Override
		public boolean containsValue( final Object val )
		{
			return multiset.containsValue( val );
		}

		@Override
		public Integer remove( final Object key )
		{
			return multiset.remove( key );
		}

		@Override
		public Set< K > keySet()
		{
			return multiset.keySet();
		}

		@Override
		public Collection< Integer > values()
		{
			return multiset.values();
		}

		@Override
		public Set< Entry< K, Integer > > entrySet()
		{
			return multiset.entrySet();
		}

		@Override
		public void clear()
		{
			multiset.clear();
		}

		@Override
		public boolean valueEquals( final Multiset< K > other )
		{
			if ( size() != other.size() )
				return false;

			for ( final Entry< K, Integer > entry : entrySet() )
			{
				final Integer otherCount = other.get( entry.getKey() );
				if ( !entry.getValue().equals( otherCount ) )
					return false;
			}

			return true;
		}

		@Override
		public Multiset< K > createVariable()
		{
			return new Multiset<>();
		}

		@Override
		public Multiset< K > copy()
		{
			final Multiset< K > copy = new Multiset<>();
			copy.putAll( this );
			return copy;
		}

		@Override
		public void set( final Multiset< K > other )
		{
			multiset.clear();
			multiset.putAll( other );
		}

		@Override
		public void add( final Multiset< K > other )
		{
			putAll( other );
		}

		@Override
		public void mul( final float c )
		{
			mul( ( double ) c );
		}

		@Override
		public void mul( final double c )
		{
			accumulatedCount = Math.round( accumulatedCount / c );
		}
	}

	static private final String basePath = System.getProperty("user.home") + "/tmp/n5-downsampling-test";
	static private final String datasetPath = "data";

	static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException
	{
		// cleanup in case the test has failed
		tearDown();

		sparkContext = new JavaSparkContext( new SparkConf()
//				.setMaster( "local[*]" )
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
			cleanup( n5Supplier.get() );
	}

	private ListImg< Multiset< Character > > createDataset( final N5Writer n5 ) throws IOException
	{
		final long[] dimensions = new long[] { 4, 4, 4 };
		final int[] cellSize = new int[] { 1, 1, 1 };

		final int numElements = ( int ) Intervals.numElements( dimensions );
		final List< Multiset< Character > > data = new ArrayList<>( numElements );
		for ( byte i = 0; i < numElements; ++i )
		{
			final Multiset< Character > multiset = new Multiset<>();
			multiset.put( Character.valueOf( ( char ) ( 'a' + i % ( 'z' - 'a' + 1 ) ) ) );
			data.add( multiset );
		}

		final ListImg< Multiset< Character > > listImg = new ListImg<>( data, dimensions );
		N5SerializableUtils.save( listImg, n5, datasetPath, cellSize, new GzipCompression() );
		return listImg;
	}

	private void cleanup( final N5Writer n5 ) throws IOException
	{
		n5.remove();
	}

	@Test
	public void testMultiset()
	{
		final Multiset< Integer > a = new Multiset<>();
		a.put( 5 );
		a.put( 3 );
		a.put( 5 );

		final Multiset< Integer > b = new Multiset<>();
		b.put( 1 );
		b.put( 2 );
		b.put( 3 );
		b.put( 4 );
		b.put( 5 );

		a.add( b );
		a.mul( 0.5f );

		Assert.assertEquals( 2, a.getAccumulatedCount() );
		Assert.assertArrayEquals( new Integer[] { 1, 2, 3, 4, 5 }, a.keySet().toArray( new Integer[ 0 ] ) );
		Assert.assertArrayEquals( new Integer[] { 1, 1, 2, 1, 3 }, a.values().toArray( new Integer[ 0 ] ) );
	}

	@Test
	public void testDownsamplingSerializable() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final ListImg< Multiset< Character > > listImg = createDataset( n5 );

		final RandomAccessibleInterval< Multiset< Character > > loaded = N5SerializableUtils.open( n5, datasetPath );
		for ( final Pair< Multiset< Character >, Multiset< Character > > pair : Views.flatIterable( Views.interval( Views.pair( listImg, loaded ), listImg ) ) )
			Assert.assertTrue( pair.getA().valueEquals( pair.getB() ) );

		final int[][] scales = N5DownsamplingSpark.downsample( sparkContext, n5Supplier, datasetPath );

		Assert.assertTrue( scales.length == 3 );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scales[ 0 ] );
		Assert.assertArrayEquals( new int[] { 2, 2, 2 }, scales[ 1 ] );
		Assert.assertArrayEquals( new int[] { 4, 4, 4 }, scales[ 2 ] );

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, downsampledAttributes.getBlockSize() );

		final Multiset< Character > expectedDownsampledMultiset = new Multiset<>();
		for ( char c = 'a'; c <= 'z'; ++c )
			expectedDownsampledMultiset.put( c, 2 );
		for ( char c = 'a'; c <= 'l'; ++c )
			expectedDownsampledMultiset.put( c );

		final RandomAccessibleInterval< Multiset< Character > > downsampledImg = N5SerializableUtils.open( n5, downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 1 }, Intervals.dimensionsAsLongArray( downsampledImg ) );
		final Cursor< Multiset< Character > > downsampledImgCursor = Views.iterable( downsampledImg ).cursor();

		Assert.assertTrue( expectedDownsampledMultiset.valueEquals( downsampledImgCursor.next() ) );

		cleanup( n5 );
	}

	@Test
	public void testIsotropicDownsampling() throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		createDataset( n5 );

		final VoxelDimensions voxelSize = new FinalVoxelDimensions( "um", 0.1, 0.1, 0.2 );
		final int[][] scales = N5DownsamplingSpark.downsampleIsotropic( sparkContext, n5Supplier, datasetPath, voxelSize );

		Assert.assertTrue( scales.length == 3 );
		Assert.assertArrayEquals( new int[] { 1, 1, 1 }, scales[ 0 ] );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, scales[ 1 ] );
		Assert.assertArrayEquals( new int[] { 4, 4, 2 }, scales[ 2 ] );

		final String downsampledIntermediateDatasetPath = Paths.get( "s1" ).toString();
		final String downsampledLastDatasetPath = Paths.get( "s2" ).toString();

		Assert.assertTrue(
				Paths.get( basePath ).toFile().listFiles( File::isDirectory ).length == 3 &&
				n5.datasetExists( datasetPath ) &&
				n5.datasetExists( downsampledIntermediateDatasetPath ) &&
				n5.datasetExists( downsampledLastDatasetPath ) );

		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 2 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 1, 1, 2 }, downsampledAttributes.getBlockSize() );

		final List< Multiset< Character > > expectedDownsampledMultisets = new ArrayList<>();

		final Multiset< Character > a = new Multiset<>();
		for ( char c = 'a'; c <= 'z'; ++c )
			a.put( c );
		for ( char c = 'a'; c <= 'f'; ++c )
			a.put( c );
		expectedDownsampledMultisets.add( a );

		final Multiset< Character > b = new Multiset<>();
		for ( char c = 'g'; c <= 'z'; ++c )
			b.put( c );
		for ( char c = 'a'; c <= 'l'; ++c )
			b.put( c );
		expectedDownsampledMultisets.add( b );

		final RandomAccessibleInterval< Multiset< Character > > downsampledImg = N5SerializableUtils.open( n5, downsampledLastDatasetPath );
		Assert.assertArrayEquals( new long[] { 1, 1, 2 }, Intervals.dimensionsAsLongArray( downsampledImg ) );
		final Cursor< Multiset< Character > > downsampledImgCursor = Views.iterable( downsampledImg ).cursor();
		final Iterator< Multiset< Character > > expectedDownsampledMultisetsIterator = expectedDownsampledMultisets.iterator();

		Assert.assertTrue( expectedDownsampledMultisetsIterator.next().valueEquals( downsampledImgCursor.next() ) );
		Assert.assertTrue( expectedDownsampledMultisetsIterator.next().valueEquals( downsampledImgCursor.next() ) );

		cleanup( n5 );
	}
}
