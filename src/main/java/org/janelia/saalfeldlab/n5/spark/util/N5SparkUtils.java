/*-
 * #%L
 * N5 Spark
 * %%
 * Copyright (C) 2017 - 2020 Igor Pisarev, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
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
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class N5SparkUtils
{
	private N5SparkUtils() { }

	public static List< Tuple2< long[], long[] > > toMinMaxTuples( final List< ? extends Interval > intervals )
	{
		return new ArrayList<>(
				intervals.stream().map( N5SparkUtils::toMinMaxTuple ).collect( Collectors.toList() )
			);
	}

	public static Tuple2< long[], long[] > toMinMaxTuple( final Interval interval )
	{
		return new Tuple2<>(
				Intervals.minAsLongArray( interval ),
				Intervals.maxAsLongArray( interval )
			);
	}

	public static Interval toInterval( final Tuple2< long[], long[] > minMaxTuple )
	{
		return new FinalInterval(
				minMaxTuple._1(),
				minMaxTuple._2()
			);
	}

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
		return openWithLoaderCache( n5, dataset, new BoundedSoftRefLoaderCache<>(cacheSize));
	}

	public static final < T extends NativeType< T > > CachedCellImg< T, ? > openWithLoaderCache(
			final N5Reader n5,
			final String dataset,
			final LoaderCache<Long, Cell<?>> loaderCache) throws IOException
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
