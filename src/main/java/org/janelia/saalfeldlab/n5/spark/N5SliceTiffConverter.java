package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.TiffUtils.TiffCompression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.CellRandomAccess;
import net.imglib2.img.cell.LazyCellImg.LazyCells;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.type.NativeType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5SliceTiffConverter
{
	/**
	 * Converts a given dataset into slice TIFF series.
	 *
	 * @param sparkContext
	 * 			Spark context
	 * @param n5
	 * 			N5 container
	 * @param datasetPath
	 * 			Path to the input dataset
	 * @param outputPath
	 * 			Path to the output folder for saving resulting TIFF series
	 * @param compression
	 * 			TIFF compression to be used for the resulting TIFF series
	 * @throws IOException
	 */
	public static < T extends NativeType< T > > void convertToSliceTiff(
			final JavaSparkContext sparkContext,
			final N5Writer n5,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = attributes.getDimensions();

		final List< Long > zCoords = LongStream.range( 0, dimensions[ 2 ] ).boxed().collect( Collectors.toList() );

		Paths.get( outputPath ).toFile().mkdirs();

		final Broadcast< N5Writer > n5Broadcast = sparkContext.broadcast( n5 );

		sparkContext.parallelize( zCoords, zCoords.size() ).foreach( z ->
			{
				final N5Reader n5Local = n5Broadcast.value();
				final CachedCellImg< T, ? > cellImg = N5SparkUtils.openWithBoundedCache( n5Local, datasetPath, 1 );
				final CellRandomAccess< T, ? extends Cell< ? > > cellImgRandomAccess = cellImg.randomAccess();
				final CellGrid cellGrid = cellImg.getCellGrid();
				final long[] zCellPos = new long[ cellImg.numDimensions() ];
				cellGrid.getCellPosition( new long[] { 0, 0, z }, zCellPos );

				final ImagePlusImg< T, ? > sliceTarget = new ImagePlusImgFactory< T >().create( new long[] { dimensions[ 0 ], dimensions[ 1 ] }, Util.getTypeFromInterval( cellImg ) );
				final RandomAccessibleInterval< T > target = Views.translate( Views.stack( sliceTarget ), new long[] { 0, 0, z } );
				final RandomAccess< T > targetRandomAccess = target.randomAccess();

				final LazyCells< ? extends Cell< ? > > cells = cellImg.getCells();
				final RandomAccessibleInterval< ? extends Cell< ? > > sliceCells = Views.interval( cells, new FinalInterval( new long[] { cells.min( 0 ), cells.min( 1 ), zCellPos[ 2 ] }, new long[] { cells.max( 0 ), cells.max( 1 ), zCellPos[ 2 ] } ) );
				final Cursor< ? extends Cell< ? > > sliceCellsCursor = Views.iterable( sliceCells ).cursor();

				while ( sliceCellsCursor.hasNext() )
				{
					final Cell< ? > cell = sliceCellsCursor.next();
					final IntervalIterator cellSliceIterator = IntervalIterator.create( new FinalInterval( new long[] { cell.min( 0 ), cell.min( 1 ), z }, new long[] { cell.min( 0 ) + cell.dimension( 0 ) - 1, cell.min( 1 ) + cell.dimension( 1 ) - 1, z } ) );
					while ( cellSliceIterator.hasNext() )
					{
						cellSliceIterator.fwd();
						cellImgRandomAccess.setPosition( cellSliceIterator );
						targetRandomAccess.setPosition( cellSliceIterator );
						targetRandomAccess.get().set( cellImgRandomAccess.get() );
					}
				}

				final ImagePlus sliceImp = sliceTarget.getImagePlus();
				final String outputImgPath = Paths.get( outputPath, z + ".tif" ).toString();
				TiffUtils.saveAsTiff( sliceImp, outputImgPath, compression );
			}
		);

		n5Broadcast.destroy();
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5SliceTiffSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5Writer n5 = N5.openFSWriter( parsedArgs.getN5Path() );
			convertToSliceTiff(
					sparkContext,
					n5,
					parsedArgs.getInputDatasetPath(),
					parsedArgs.getOutputPath(),
					parsedArgs.getTiffCompression()
				);
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments
	{
		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = { "--outputPath" }, required = true,
				usage = "Output path for storing slice TIFF series.")
		private String outputPath;

		@Option(name = "-c", aliases = { "--tiffCompression" }, required = false,
				usage = "Tiff compression (LZW or NONE).")
		private TiffCompression tiffCompression = TiffCompression.LZW;

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
	}
}
