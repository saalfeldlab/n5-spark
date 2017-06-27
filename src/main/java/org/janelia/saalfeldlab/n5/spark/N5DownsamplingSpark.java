package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.export.Downsample;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;

public class N5DownsamplingSpark< T extends NativeType< T > & RealType< T > >
{
	private final transient JavaSparkContext sparkContext;

	/**
	 * Takes an existing spark context to run the downsampling code.
	 * Assumes that the spark context is instantiated using Kryo serializer (see {@link N5DownsamplingSparkTest} for reference)
	 * as the downsampling code serializes objects of type {@link Interval}.
	 *
	 * @param sparkContext
	 */
	public N5DownsamplingSpark( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}

	/**
	 * Generates lower scale levels for a given dataset. Each scale level is downsampled by 2 in all dimensions.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * Reuses the block size of the given dataset.
	 *
	 * @param basePath Path to the N5 root
	 * @param datasetPath Path to the full-scale dataset
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	public double[][] downsample( final String basePath, final String datasetPath ) throws IOException
	{
		// TODO: do not generate intermediate downsampled XY exports
		return downsampleIsotropic( basePath, datasetPath, null );
	}

	/**
	 * <p>
	 * Generates lower scale levels for a given dataset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * </p><p>
	 * Assumes that the pixel resolution is the same in X and Y.
	 * Each scale level is downsampled by 2 in XY, and by the corresponding factors in Z to be as close as possible to isotropic.
	 * Reuses the block size of the given dataset, and adjusts the block sizes in Z to be consistent with the scaling factors.
	 * </p>
	 *
	 * @param basePath Path to the N5 root
	 * @param datasetPath Path to the full-scale dataset
	 * @param voxelDimensions Pixel resolution of the data
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	public double[][] downsampleIsotropic( final String basePath, final String datasetPath, final VoxelDimensions voxelDimensions ) throws IOException
	{
		final double pixelResolutionZToXY = ( voxelDimensions != null ? getPixelResolutionZtoXY( voxelDimensions ) : 1 );

		final List< int[] > scalesXY = downsampleXY( basePath, datasetPath );
		final List< int[] > scalesZ  = downsampleZ ( basePath, datasetPath, pixelResolutionZToXY );
		deleteXY( basePath, datasetPath );

		final List< double[] > scales = new ArrayList<>();
		scales.add( new double[] { 1, 1, 1 } );
		for ( int s = 0; s < Math.min( scalesXY.size(), scalesZ.size() ); ++s )
			scales.add( new double[] { scalesXY.get( s )[ 0 ], scalesXY.get( s )[ 1 ], scalesZ.get( s )[ 2 ] } );
		return scales.toArray( new double[ 0 ][] );
	}

	// TODO: unify with downsampleZ as these two methods share a lot of similar code
	private List< int[] > downsampleXY(final String basePath, final String datasetPath ) throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] fullScaleDimensions = attributes.getDimensions();
		final int[] cellSize = attributes.getBlockSize();
		final int dim = fullScaleDimensions.length;

		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		final String xyGroupPath = Paths.get( rootOutputPath, "xy" ).toString();
		n5.createGroup( xyGroupPath );

		String previousScaleLevel = datasetPath;
		final long[] previousDimensions = fullScaleDimensions.clone();
		final long[] downsampledDimensions = new long[ dim ];

		final List< int[] > downsamplingFactorsForScales = new ArrayList<>();

		// loop over scale levels
		for ( int scaleLevel = 1; ; ++scaleLevel )
		{
			for ( int i = 0; i < 2; i++ )
				downsampledDimensions[ i ] = fullScaleDimensions[ i ] >> scaleLevel;
			System.arraycopy( fullScaleDimensions, 2, downsampledDimensions, 2, fullScaleDimensions.length - 2 );

			if ( Math.min( downsampledDimensions[ 0 ], downsampledDimensions[ 1 ] ) <= 1 || Math.max( downsampledDimensions[ 0 ], downsampledDimensions[ 1 ] ) <= Math.max( cellSize[ 0 ], cellSize[ 1 ] ) )
				break;

			final String scaleLevelPath = Paths.get( xyGroupPath, "s" + scaleLevel ).toString();
			n5.createDataset( scaleLevelPath, downsampledDimensions, cellSize, attributes.getDataType(), attributes.getCompressionType() );

			final List< Tuple2< Interval, Interval > > sourceAndTargetIntervals = new ArrayList<>();
			final long[] max = Intervals.maxAsLongArray( new FinalInterval( downsampledDimensions ) );
			final long[] offset = new long[ dim ], sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			for ( int d = 0; d < dim; )
			{
				for ( int i = 0; i < 2; i++ )
				{
					targetMin[ i ] = offset[ i ];
					targetMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
					sourceMin[ i ] = targetMin[ i ] * 2;
					sourceMax[ i ] = targetMax[ i ] * 2 + 1;
				}
				for ( int i = 2; i < dim; i++ )
				{
					targetMin[ i ] = sourceMin[ i ] = offset[ i ];
					targetMax[ i ] = sourceMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
				}

				sourceAndTargetIntervals.add( new Tuple2<>( new FinalInterval( sourceMin, sourceMax ), new FinalInterval( targetMin, targetMax ) ) );

				for ( d = 0; d < dim; ++d )
				{
					offset[ d ] += cellSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}

			final int[] downsamplingFactors = new int[ dim ];
			Arrays.fill( downsamplingFactors, 1 );
			for ( int i = 0; i < 2; i++ )
				downsamplingFactors[ i ] = 2;

			downsamplingFactorsForScales.add( downsamplingFactors );

			final String previousScaleLevelSpark = previousScaleLevel;
			sparkContext.parallelize( sourceAndTargetIntervals ).foreach( sourceAndTargetInterval ->
			{
				final N5Writer n5Local = N5.openFSWriter( basePath );

				// TODO: can skip this target block if all source blocks are empty (not present)

				final RandomAccessibleInterval< T > previousScaleLevelImg = N5Utils.open( n5Local, previousScaleLevelSpark );
				final RandomAccessibleInterval< T > source = Views.offsetInterval( previousScaleLevelImg, sourceAndTargetInterval._1() );
				final Img< T > target = new ArrayImgFactory< T >().create( sourceAndTargetInterval._2(), Util.getTypeFromInterval( source ) );
				Downsample.downsample( source, target, downsamplingFactors );

				final long[] gridPosition = new long[ dim ];
				final CellGrid cellGrid = new CellGrid( downsampledDimensions, cellSize );
				cellGrid.getCellPosition( Intervals.minAsLongArray( sourceAndTargetInterval._2() ), gridPosition );

				N5Utils.saveBlock( target, n5Local, scaleLevelPath, gridPosition );
			} );

			previousScaleLevel = scaleLevelPath;
			System.arraycopy( downsampledDimensions, 0, previousDimensions, 0, downsampledDimensions.length );
		}

		return downsamplingFactorsForScales;
	}

	// TODO: unify with downsampleXY as these two methods share a lot of similar code
	private List< int[] > downsampleZ(final String basePath, final String datasetPath, final double pixelResolutionZToXY ) throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		final String xyGroupPath = Paths.get( rootOutputPath, "xy" ).toString();

		final List< int[] > downsamplingFactorsForScales = new ArrayList<>();

		// loop over scale levels
		for ( int scaleLevel = 1; ; ++scaleLevel )
		{
			final String xyScaleLevelPath = Paths.get( xyGroupPath, "s" + scaleLevel ).toString();
			final String scaleLevelPath = Paths.get( rootOutputPath, "s" + scaleLevel ).toString();

			if ( !n5.datasetExists( xyScaleLevelPath ) )	// XY limit reached
				break;

			final DatasetAttributes attributes = n5.getDatasetAttributes( xyScaleLevelPath );
			final long[] xyScaleDimensions = attributes.getDimensions();
			final int[] xyCellSize = attributes.getBlockSize();
			final int dim = xyScaleDimensions.length;

			final Tuple2< Integer, Integer > zCellSizeAndDownsamplingFactor = getOptimalZCellSizeAndDownsamplingFactor( scaleLevel, Math.max( xyCellSize[ 0 ], xyCellSize[ 1 ] ), pixelResolutionZToXY );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 2; d < downsampledDimensions.length; ++d )
				downsampledDimensions[ d ] = xyScaleDimensions[ d ] / zCellSizeAndDownsamplingFactor._2();
			System.arraycopy( xyScaleDimensions, 0, downsampledDimensions, 0, 2 );

			final int[] cellSize = new int[ dim ];
			Arrays.fill( cellSize, zCellSizeAndDownsamplingFactor._1() );
			System.arraycopy( xyCellSize, 0, cellSize, 0, 2 );

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() <= 1 || Arrays.stream( downsampledDimensions ).max().getAsLong() <= Math.max( cellSize[ 0 ], cellSize[ 1 ] ) )
				break;

			n5.createDataset( scaleLevelPath, downsampledDimensions, cellSize, attributes.getDataType(), attributes.getCompressionType() );

			final List< Tuple2< Interval, Interval > > sourceAndTargetIntervals = new ArrayList<>();
			final long[] max = Intervals.maxAsLongArray( new FinalInterval( downsampledDimensions ) );
			final long[] offset = new long[ dim ], sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			for ( int d = 0; d < dim; )
			{
				for ( int i = 0; i < 2; i++ )
				{
					targetMin[ i ] = sourceMin[ i ] = offset[ i ];
					targetMax[ i ] = sourceMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
				}
				for ( int i = 2; i < dim; i++ )
				{
					targetMin[ i ] = offset[ i ];
					targetMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
					sourceMin[ i ] = targetMin[ i ] * zCellSizeAndDownsamplingFactor._2();
					sourceMax[ i ] = targetMax[ i ] * zCellSizeAndDownsamplingFactor._2() + ( zCellSizeAndDownsamplingFactor._2() - 1 );
				}

				sourceAndTargetIntervals.add( new Tuple2<>( new FinalInterval( sourceMin, sourceMax ), new FinalInterval( targetMin, targetMax ) ) );

				for ( d = 0; d < dim; ++d )
				{
					offset[ d ] += cellSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}

			final int[] downsamplingFactors = new int[ dim ];
			Arrays.fill( downsamplingFactors, 1 );
			for ( int i = 2; i < dim; ++i )
				downsamplingFactors[ i ] = zCellSizeAndDownsamplingFactor._2();

			downsamplingFactorsForScales.add( downsamplingFactors );

			sparkContext.parallelize( sourceAndTargetIntervals ).foreach( sourceAndTargetInterval ->
			{
				final N5Writer n5Local = N5.openFSWriter( basePath );

				// TODO: can skip this target block if all source blocks are empty (not present)

				final RandomAccessibleInterval< T > xyScaleLevelImg = N5Utils.open( n5Local, xyScaleLevelPath );
				final RandomAccessibleInterval< T > source = Views.offsetInterval( xyScaleLevelImg, sourceAndTargetInterval._1() );
				final Img< T > target = new ArrayImgFactory< T >().create( sourceAndTargetInterval._2(), Util.getTypeFromInterval( source ) );
				Downsample.downsample( source, target, downsamplingFactors );

				final long[] gridPosition = new long[ dim ];
				final CellGrid cellGrid = new CellGrid( downsampledDimensions, cellSize );
				cellGrid.getCellPosition( Intervals.minAsLongArray( sourceAndTargetInterval._2() ), gridPosition );

				N5Utils.saveBlock( target, n5Local, scaleLevelPath, gridPosition );
			} );
		}

		return downsamplingFactorsForScales;
	}

	private void deleteXY( final String basePath, final String datasetPath ) throws IOException
	{
		// TODO: parallelized deletion of temporary export folder
		final String rootOutputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		final String xyGroupPath = Paths.get( rootOutputPath, "xy" ).toString();
		final N5Writer n5 = N5.openFSWriter( basePath );
		n5.remove( xyGroupPath );
	}

	private static double getPixelResolutionZtoXY( final VoxelDimensions voxelDimensions )
	{
		return voxelDimensions.dimension( 2 ) / Math.max( voxelDimensions.dimension( 0 ), voxelDimensions.dimension( 1 ) );
	}

	private static Tuple2< Integer, Integer > getOptimalZCellSizeAndDownsamplingFactor( final int scaleLevel, final int cellSizeXY, final double pixelResolutionZToXY )
	{
		final int isotropicScaling = ( int ) Math.round( ( 1 << scaleLevel ) / pixelResolutionZToXY );
		final int zDownsamplingFactor = Math.max( isotropicScaling, 1 );
		final int fullScaleOptimalCellSize = ( int ) Math.round( cellSizeXY / pixelResolutionZToXY );
		final int zCellSize = ( fullScaleOptimalCellSize << scaleLevel ) / zDownsamplingFactor;
		return new Tuple2<>( zCellSize, zDownsamplingFactor );
	}
}
