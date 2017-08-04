package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.TiffUtils.TiffCompression;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;

public class N5MaxIntensityProjection
{
	private static class MipKey
	{
		private final String key;

		public final int dimension;
		public final int mipStep;
		public final long[] coords;

		public MipKey( final int dimension, final int mipStep )
		{
			this( dimension, mipStep, null );
		}

		public MipKey( final int dimension, final int mipStep, final long[] coords )
		{
			this.dimension = dimension;
			this.mipStep = mipStep;
			this.coords = coords;

			key = dimension + ":" + mipStep + ( coords != null ? "=" + Arrays.toString( coords ) : "" );
		}

		@Override
		public String toString()
		{
			return key;
		}

		@Override
		public boolean equals( final Object obj )
		{
			if ( obj instanceof MipKey )
				return key.equals( ( ( MipKey ) obj ).key );
			else
				return super.equals( obj );
		}

		@Override
		public int hashCode()
		{
			return key.hashCode();
		}
	}

	private static final int MAX_PARTITIONS = 15000;

	private static final String[] AXES = new String[] { "x", "y", "z" };

	public static < T extends NativeType< T > & RealType< T > > void createMaxIntensityProjection(
			final JavaSparkContext sparkContext,
			final String basePath,
			final String datasetPath,
			final int[] cellsInSingleMIP,
			final String outputPath,
			final TiffCompression compression ) throws IOException
	{
		final N5Reader n5 = N5.openFSReader( basePath );
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final int dim = dimensions.length;
		if ( dim > 3 )
			throw new RuntimeException( "MaxIntensityProjection is supported for 1D/2D/3D datasets" );

		final long numCells;
		final T type;
		{
			final CachedCellImg< T, ? > cellImg = N5SparkUtils.openWithBoundedCache( n5, datasetPath, 1 );
			final long[] cellGridDimensions = cellImg.getCellGrid().getGridDimensions();
			numCells = Intervals.numElements( cellGridDimensions );
			type = Util.getTypeFromInterval( cellImg ).createVariable();
		}

		for ( int d = 0; d < dim; ++d )
			Paths.get( outputPath, AXES[ d ] ).toFile().mkdirs();

		final Broadcast< T > broadcastedType = sparkContext.broadcast( type );

		sparkContext
			// distribute flat cell indexes
			.parallelize(
					LongStream.range( 0, numCells ).boxed().collect( Collectors.toList() ),
					Math.min( ( int ) numCells, MAX_PARTITIONS )
				)
			// compute MIPs for x/y/z of each cell
			.flatMapToPair( cellIndex ->
					{
						final N5Reader n5Local = N5.openFSReader( basePath );
						final CachedCellImg< T, ? > cellImg = N5SparkUtils.openWithBoundedCache( n5Local, datasetPath, 1 );
						final RandomAccess< T > cellImgRandomAccess = cellImg.randomAccess();

						final long[] cellMin = new long[ dim ], cellMax = new long[ dim ];
						final int[] cellDims = new int[ dim ];
						final long[] cellGridPosition = new long[ dim ];
						final CellGrid cellGrid = cellImg.getCellGrid();
						cellGrid.getCellGridPositionFlat( cellIndex, cellGridPosition );
						cellGrid.getCellDimensions( cellGridPosition, cellMin, cellDims );
						for ( int d = 0; d < dim; ++d )
							cellMax[ d ] = cellMin[ d ] + cellDims[ d ] - 1;

						final List< RandomAccessibleInterval< T > > cellMips = new ArrayList<>();
						final List< RandomAccess< T > > cellMipsRandomAccess = new ArrayList<>();
						for ( int d = 0; d < dim; ++d )
						{
							final RandomAccessibleInterval< T > cellMip = Views.translate(
									new ArrayImgFactory< T >().create(
											getMipPosition(
													Intervals.dimensionsAsLongArray( new FinalDimensions( cellDims ) ), // ugly conversion from int[] to long[]
													d
												),
											broadcastedType.value()
										),
									getMipPosition( cellMin, d )
								);
							cellMips.add( cellMip );
							cellMipsRandomAccess.add( cellMip.randomAccess() );
						}

						final IntervalIterator cellIterator = new IntervalIterator( cellMin, cellMax );
						final long[] cellPos = new long[ cellIterator.numDimensions() ];
						while ( cellIterator.hasNext() )
						{
							cellIterator.fwd();
							cellIterator.localize( cellPos );
							cellImgRandomAccess.setPosition( cellPos );
							final double cellVal = cellImgRandomAccess.get().getRealDouble();
							for ( int d = 0; d < dim; ++d )
							{
								final RandomAccess< T > cellMipRandomAccess = cellMipsRandomAccess.get( d );
								final long[] cellMipPos = getMipPosition( cellPos, d );
								cellMipRandomAccess.setPosition( cellMipPos );
								final double cellMipVal = cellMipRandomAccess.get().getRealDouble();
								cellMipRandomAccess.get().setReal( Math.max( cellVal, cellMipVal ) );
							}
						}

						final List< Tuple2< MipKey, RandomAccessibleInterval< T > > > ret = new ArrayList<>();
						for ( int d = 0; d < dim; ++d )
						{
							final int mipStep = ( int) ( cellGridPosition[ d ] / cellsInSingleMIP[ d ] );
							ret.add( new Tuple2<>( new MipKey( d, mipStep, getMipPosition( cellGridPosition, d ) ), cellMips.get( d ) ) );
						}
						return ret.iterator();
					}
				)
			// join all cells on top of each other that should go to the same MIP
			.reduceByKey( ( mip1, mip2 ) ->
					{
						final Cursor< T > mip1Cursor = Views.flatIterable( mip1 ).cursor();
						final Cursor< T > mip2Cursor = Views.flatIterable( mip2 ).cursor();
						while ( mip1Cursor.hasNext() || mip2Cursor.hasNext() )
						{
							mip1Cursor.fwd();
							mip2Cursor.fwd();
							mip1Cursor.get().setReal( Math.max( mip1Cursor.get().getRealDouble(), mip2Cursor.get().getRealDouble() ) );
						}
						return mip1;
					}
				)
			// group by dimension and MIP index
			.mapToPair( keyAndMip -> new Tuple2<>( new MipKey( keyAndMip._1().dimension, keyAndMip._1().mipStep ), keyAndMip._2() ) )
			.groupByKey()
			// join cells into a single MIP for each dimension and MIP index
			.foreach( keyAndMips ->
					{
						final int mipDimension = keyAndMips._1().dimension;
						final long mipCoordinate = ( long ) keyAndMips._1().mipStep * cellsInSingleMIP[ mipDimension ] * blockSize[ mipDimension ];

						final ImagePlusImg< T, ? > mip = new ImagePlusImgFactory< T >().create(
								getMipPosition( dimensions, mipDimension ),
								broadcastedType.value()
							);

						for ( final RandomAccessibleInterval< T > cellMip : keyAndMips._2() )
						{
							final RandomAccessibleInterval< T > mipInterval = Views.interval( mip, cellMip );
							final Cursor< T > mipCursor = Views.flatIterable( mipInterval ).cursor();
							final Cursor< T > cellMipCursor = Views.flatIterable( cellMip ).cursor();
							while ( mipCursor.hasNext() || cellMipCursor.hasNext() )
								mipCursor.next().set( cellMipCursor.next() );
						}

						final ImagePlus mipImp = mip.getImagePlus();
						final String outputMipPath = Paths.get( outputPath, AXES[ mipDimension ], mipCoordinate + ".tif" ).toString();
						TiffUtils.saveAsTiff( mipImp, outputMipPath, compression );
					}
				);

		broadcastedType.destroy();
	}

	private static long[] getMipPosition( final long[] pos, final int mipDim )
	{
		final long[] mipPos = new long[ pos.length - 1 ];
		System.arraycopy( pos, 0, mipPos, 0, mipDim );
		System.arraycopy( pos, mipDim + 1, mipPos, mipDim, mipPos.length - mipDim );
		return mipPos;
	}
}
