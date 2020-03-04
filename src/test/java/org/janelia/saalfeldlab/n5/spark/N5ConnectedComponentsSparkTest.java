package org.janelia.saalfeldlab.n5.spark;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class N5ConnectedComponentsSparkTest extends AbstractN5SparkTest
{
    static private final String datasetPath = "data";
    static private final String relabeledDatasetPath = "data-relabeled";

    @Test
    public void test2D_blockSize1() throws IOException
    {
        runTest2D( new int[] { 4, 4 } );
    }

    @Test
    public void test2D_blockSize2() throws IOException
    {
        runTest2D( new int[] { 5, 2 } );
    }

    @Test
    public void test2D_blockSize3() throws IOException
    {
        runTest2D( new int[] { 1, 2 } );
    }

    @Test
    public void test3D_blockSize1() throws IOException
    {
        runTest3D( new int[] { 1, 1, 1 } );
    }

    @Test
    public void test3D_blockSize2() throws IOException
    {
        runTest3D( new int[] { 2, 2, 2 } );
    }

    @Test
    public void test3D_blockSize3() throws IOException
    {
        runTest3D( new int[] { 3, 4, 5 } );
    }

    private void runTest2D( final int[] blockSize ) throws IOException
    {
        final N5Writer n5 = new N5FSWriter( basePath );
        final long[] dimensions = new long[] { 15, 4 };

        final int[] data = {
                0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 0,
                0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 1,
                0, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0,
                0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0,
        };

        N5Utils.save( ArrayImgs.ints( data, dimensions ), n5, datasetPath, blockSize, new GzipCompression() );

        N5ConnectedComponentsSpark.connectedComponents(
                sparkContext,
                () -> new N5FSWriter( basePath ),
                datasetPath,
                relabeledDatasetPath,
                Optional.empty(),
                Optional.empty() );

        Assert.assertTrue( n5.datasetExists( datasetPath ) );
        Assert.assertTrue( n5.datasetExists( relabeledDatasetPath ) );

        final int[] expected = data;

        final RandomAccessibleInterval< UnsignedLongType > output = N5Utils.open( n5, relabeledDatasetPath );
        Assert.assertArrayEquals( expected, relabel( output ) );
    }

    private void runTest3D( final int[] blockSize ) throws IOException
    {
        final N5Writer n5 = new N5FSWriter( basePath );
        final long[] dimensions = new long[] { 4, 4, 3 };

        final int[] data = {
                0, 1, 1, 0,
                1, 0, 1, 1,
                1, 1, 1, 0,
                0, 1, 0, 1,

                0, 1, 0, 1,
                0, 1, 1, 0,
                1, 0, 1, 0,
                0, 0, 0, 1,

                1, 1, 0, 1,
                0, 0, 0, 1,
                1, 1, 1, 0,
                0, 1, 0, 1,
        };

        N5Utils.save( ArrayImgs.ints( data, dimensions ), n5, datasetPath, blockSize, new GzipCompression() );

        N5ConnectedComponentsSpark.connectedComponents(
                sparkContext,
                () -> new N5FSWriter( basePath ),
                datasetPath,
                relabeledDatasetPath,
                Optional.empty(),
                Optional.empty() );

        Assert.assertTrue( n5.datasetExists( datasetPath ) );
        Assert.assertTrue( n5.datasetExists( relabeledDatasetPath ) );

        final int[] expected = {
                0, 1, 1, 0,
                1, 0, 1, 1,
                1, 1, 1, 0,
                0, 1, 0, 2,

                0, 1, 0, 3,
                0, 1, 1, 0,
                1, 0, 1, 0,
                0, 0, 0, 2,

                1, 1, 0, 3,
                0, 0, 0, 3,
                1, 1, 1, 0,
                0, 1, 0, 2,
        };

        final RandomAccessibleInterval< UnsignedLongType > output = N5Utils.open( n5, relabeledDatasetPath );
        Assert.assertArrayEquals( expected, relabel( output ) );
    }

    private int[] relabel( final RandomAccessibleInterval< ? extends IntegerType<?>> labelsImg )
    {
        final int[] relabeledData = new int[ ( int ) Intervals.numElements( labelsImg ) ];
        int index = 0;
        final Map< Long, Integer > mapping = new HashMap<>();

        final Cursor< ? extends IntegerType<?>> outputCursor = Views.flatIterable( labelsImg ).cursor();
        while ( outputCursor.hasNext() )
        {
            final long id = outputCursor.next().getIntegerLong();
            if ( id != 0 )
            {
                if ( !mapping.containsKey( id ) )
                    mapping.put( id, mapping.size() + 1 );
                relabeledData[ index ] = mapping.get( id );
            }
            ++index;
        }

        return relabeledData;
    }
}
