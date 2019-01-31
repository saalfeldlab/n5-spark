package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5SparkUtils;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils.TiffCompression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

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

		public MipKey( final int dimension, final int mipStep )
		{
			this( dimension, mipStep, null );
		}

		public MipKey( final int dimension, final int mipStep, final long[] coords )
		{
			this.dimension = dimension;
			this.mipStep = mipStep;

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

	/**
	 * Generates max intensity projection of the given dataset in X/Y/Z directions.
	 * Saves the resulting MIPs as TIFF images in the specified output folder.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting MIPs
	 * @param compression
	 * 			TIFF compression to be used for the resulting MIPs
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void createMaxIntensityProjection(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression ) throws IOException
	{
		createMaxIntensityProjection(
				sparkContext,
				n5Supplier,
				datasetPath,
				null,
				outputPath,
				compression
			);
	}

	/**
	 * Generates max intensity projection of the given dataset in X/Y/Z directions using the specified MIP step.
	 * Saves the resulting MIPs as TIFF images in the specified output folder.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Reader} supplier
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param cellsInSingleMIP
	 * 			MIP step in X/Y/Z directions specified as the number of N5 blocks included in a single MIP
	 * @param outputPath
	 * 			Path to the output folder for saving resulting MIPs
	 * @param compression
	 * 			TIFF compression to be used for the resulting MIPs
	 * @throws IOException
	 */
	public static < T extends NativeType< T > & RealType< T > > void createMaxIntensityProjection(
			final JavaSparkContext sparkContext,
			final N5ReaderSupplier n5Supplier,
			final String datasetPath,
			final int[] cellsInSingleMIP,
			final String outputPath,
			final TiffCompression compression ) throws IOException
	{
		final N5Reader n5 = n5Supplier.get();
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

		final Broadcast< T > typeBroadcast = sparkContext.broadcast( type );

		sparkContext
			// distribute flat cell indexes
			.parallelize(
					LongStream.range( 0, numCells ).boxed().collect( Collectors.toList() ),
					Math.min( ( int ) numCells, MAX_PARTITIONS )
				)
			// compute MIPs for x/y/z of each cell
			.flatMapToPair( cellIndex ->
					{
						final N5Reader n5Local = n5Supplier.get();
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
									new ArrayImgFactory<>( typeBroadcast.value() ).create(
											getMipPosition(
													Intervals.dimensionsAsLongArray( new FinalDimensions( cellDims ) ), // ugly conversion from int[] to long[]
													d
												)
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
							final int mipStep = cellsInSingleMIP == null ? 0 : ( int ) ( cellGridPosition[ d ] / cellsInSingleMIP[ d ] );
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
						final long mipCoordinate = cellsInSingleMIP == null ? 0 : ( long ) keyAndMips._1().mipStep * cellsInSingleMIP[ mipDimension ] * blockSize[ mipDimension ];

						final ImagePlusImg< T, ? > mip = new ImagePlusImgFactory<>( typeBroadcast.value() ).create(
								getMipPosition( dimensions, mipDimension )
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

		typeBroadcast.destroy();
	}

	private static long[] getMipPosition( final long[] pos, final int mipDim )
	{
		final long[] mipPos = new long[ pos.length - 1 ];
		System.arraycopy( pos, 0, mipPos, 0, mipDim );
		System.arraycopy( pos, mipDim + 1, mipPos, mipDim, mipPos.length - mipDim );
		return mipPos;
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5MaxIntensityProjectionSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5ReaderSupplier n5Supplier = () -> new N5FSReader( parsedArgs.getN5Path() );
			createMaxIntensityProjection(
					sparkContext,
					n5Supplier,
					parsedArgs.getInputDatasetPath(),
					parsedArgs.getMipCellsStep(),
					parsedArgs.getOutputPath(),
					parsedArgs.getTiffCompression()
				);
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = 4847292347478989514L;

		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--outputPath" }, required = true,
				usage = "Output path for storing TIFF max intensity projections.")
		private String outputPath;

		@Option(name = "-c", aliases = { "--tiffCompression" }, required = false,
				usage = "Tiff compression (not used by default)."
						+ "WARNING: LZW compressor can be very slow. It is not recommended for general use unless saving disk space is crucial.")
		private TiffCompression tiffCompression = TiffCompression.NONE;

		@Option(name = "-m", aliases = { "--mipCellsStep" }, required = false,
				usage = "Number of cells used for a single MIP image (MIP step in X/Y/Z). By default the MIP is computed through the entire volume.")
		private String mipCellsStep;

		private boolean parsedSuccessfully = false;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public boolean parsedSuccessfully() { return parsedSuccessfully; }

		public String getN5Path() { return n5Path; }
		public String getInputDatasetPath() { return inputDatasetPath; }
		public String getOutputPath() { return outputPath; }
		public TiffCompression getTiffCompression() { return tiffCompression; }
		public int[] getMipCellsStep() { return CmdUtils.parseIntArray( mipCellsStep ); }
	}
}
