package org.janelia.saalfeldlab.n5.spark.util;

import java.io.IOException;
import java.util.Set;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;

import net.imglib2.cache.Cache;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;

public class N5SparkUtils
{
	private N5SparkUtils() { }

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with bounded cache size.
	 *
	 * @param n5
	 * @param dataset
	 * @param cacheSize
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > CachedCellImg< T, ? > openWithBoundedCache(
			final N5Reader n5,
			final String dataset,
			final int cacheSize ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader< T > loader = new N5CellLoader<>( n5, dataset, blockSize );

		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final CachedCellImg< T, ? > img;
		final T type;
		final Cache< Long, Cell< ? > > cache;
		final Set< AccessFlags > accessFlags = AccessFlags.setOf();

		final LoaderCache< Long, Cell< ? > > loaderCache = new BoundedSoftRefLoaderCache<>( cacheSize );

		switch ( attributes.getDataType() )
		{
		case INT8:
			type = ( T )new ByteType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.BYTE, accessFlags ) );
			break;
		case UINT8:
			type = ( T )new UnsignedByteType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.BYTE, accessFlags ) );
			break;
		case INT16:
			type = ( T )new ShortType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.SHORT, accessFlags ) );
			break;
		case UINT16:
			type = ( T )new UnsignedShortType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.SHORT, accessFlags ) );
			break;
		case INT32:
			type = ( T )new IntType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.INT, accessFlags ) );
			break;
		case UINT32:
			type = ( T )new UnsignedIntType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.INT, accessFlags ) );
			break;
		case INT64:
			type = ( T )new LongType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.LONG, accessFlags ) );
			break;
		case UINT64:
			type = ( T )new UnsignedLongType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.LONG, accessFlags ) );
			break;
		case FLOAT32:
			type = ( T )new FloatType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.FLOAT, accessFlags ) );
			break;
		case FLOAT64:
			type = ( T )new DoubleType();
			cache = loaderCache.withLoader( ( CacheLoader )LoadedCellCacheLoader.get( grid, loader, type, accessFlags ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.DOUBLE, accessFlags ) );
			break;
		default:
			img = null;
		}

		return img;
	}
}
