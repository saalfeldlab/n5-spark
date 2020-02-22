package org.janelia.saalfeldlab.n5.spark;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class N5ConnectedComponentsSparkTest
{
    static private final String basePath = System.getProperty("user.home") + "/.n5-spark-test-" + RandomStringUtils.randomAlphanumeric(5);
    static private final String datasetPath = "data";
    static private final String relabeledDatasetPath = "data-relabeled";

    static private final N5WriterSupplier n5Supplier = () -> new N5FSWriter( basePath );

    private JavaSparkContext sparkContext;

    @Before
    public void setUp() throws IOException
    {
        // cleanup in case the test has failed
        tearDown();

        sparkContext = new JavaSparkContext( new SparkConf()
                .setMaster( "local[*]" )
                .setAppName( "N5ConnectedComponentsTest" )
                .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
        );
    }

    @After
    public void tearDown() throws IOException
    {
        if ( sparkContext != null )
            sparkContext.close();

        if ( Files.exists( Paths.get( basePath ) ) )
            Assert.assertTrue( n5Supplier.get().remove() );
    }

    @Test
    public void test1() throws IOException
    {
        runTest( new int[] { 1, 1, 1 } );
    }

    @Test
    public void test2() throws IOException
    {
        runTest( new int[] { 2, 2, 2 } );
    }

    @Test
    public void test3() throws IOException
    {
        runTest( new int[] { 3, 4, 5 } );
    }

    private void runTest( final int[] blockSize ) throws IOException
    {
        final N5Writer n5 = n5Supplier.get();
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
                n5Supplier,
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
