package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5SliceTiffConverter
{
	public static < T extends NativeType< T > > void convertToSliceTiff(
			final JavaSparkContext sparkContext,
			final String basePath,
			final String datasetPath,
			final String outputPath ) throws IOException
	{
		final N5Reader n5 = N5.openFSReader( basePath );
		final DatasetAttributes attributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = attributes.getDimensions();

		final long[] sliceDimensions = new long[] { dimensions[ 0 ], dimensions[ 1 ] };
		final List< Integer > zCoords = new ArrayList<>();
		for ( int z = 0; z < dimensions[ 2 ]; ++z )
			zCoords.add( z );

		Paths.get( outputPath ).toFile().mkdirs();

		sparkContext.parallelize( zCoords, zCoords.size() ).foreach( z ->
			{
				final N5Reader n5Local = N5.openFSReader( basePath );
				final RandomAccessibleInterval< T > img = N5Utils.open( n5Local, datasetPath );
				final RandomAccessibleInterval< T > sliceSource = Views.hyperSlice( img, 2, z );
				final ImagePlusImg< T, ? > sliceTarget = new ImagePlusImgFactory< T >().create( sliceDimensions, Util.getTypeFromInterval( sliceSource ) );
				final Cursor< T > sliceSourceCursor = Views.iterable( sliceSource ).localizingCursor();
				final RandomAccess< T > sliceTargetRandomAccess = sliceTarget.randomAccess();
				while ( sliceSourceCursor.hasNext() )
				{
					sliceSourceCursor.fwd();
					sliceTargetRandomAccess.setPosition( sliceSourceCursor );
					sliceTargetRandomAccess.get().set( sliceSourceCursor.get() );
				}
				final ImagePlus sliceImp = sliceTarget.getImagePlus();
				IJ.saveAsTiff( sliceImp, Paths.get( outputPath, z + ".tif" ).toString() );
			} );
	}
}
