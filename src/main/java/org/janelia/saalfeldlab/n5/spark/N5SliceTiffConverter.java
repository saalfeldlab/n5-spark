package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;

import ij.IJ;
import ij.ImagePlus;
import loci.plugins.LociExporter;
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
	public static enum TiffCompression
	{
		UNCOMPRESSED,
		LZW
	}

	public static < T extends NativeType< T > > void convertToSliceTiff(
			final JavaSparkContext sparkContext,
			final String basePath,
			final String datasetPath,
			final String outputPath,
			final TiffCompression compression ) throws IOException
	{
		final N5Reader n5 = N5.openFSReader( basePath );
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = attributes.getDimensions();

		final List< Long > zCoords = LongStream.range( 0, dimensions[ 2 ] ).boxed().collect( Collectors.toList() );

		Paths.get( outputPath ).toFile().mkdirs();

		sparkContext.parallelize( zCoords, zCoords.size() ).foreach( z ->
			{
				final N5Reader n5Local = N5.openFSReader( basePath );
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
				switch ( compression )
				{
				case UNCOMPRESSED:
					IJ.saveAsTiff( sliceImp, outputImgPath );
					break;
				case LZW:
					final LociExporter lociExporter = new LociExporter();
					lociExporter.setup( String.format( "outfile=[%s] compression=[LZW] windowless=[TRUE]", outputImgPath ), sliceImp );
					lociExporter.run( null );
				default:
					break;
				}
			}
		);
	}
}
