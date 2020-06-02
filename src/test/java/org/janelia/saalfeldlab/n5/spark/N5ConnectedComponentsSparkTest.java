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
import java.util.*;

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

        final Map< N5ConnectedComponentsSpark.NeighborhoodShapeType, int[] > shapeTypeAndExpected = new HashMap<>();
        shapeTypeAndExpected.put( N5ConnectedComponentsSpark.NeighborhoodShapeType.Diamond, data ); // single component
        shapeTypeAndExpected.put( N5ConnectedComponentsSpark.NeighborhoodShapeType.Box, data ); // single component

        runTest( n5, shapeTypeAndExpected );
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

        final int[] expectedForDiamondNeighborhoodShapeType = {
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

        final Map< N5ConnectedComponentsSpark.NeighborhoodShapeType, int[] > shapeTypeAndExpected = new HashMap<>();
        shapeTypeAndExpected.put( N5ConnectedComponentsSpark.NeighborhoodShapeType.Diamond, expectedForDiamondNeighborhoodShapeType );
        shapeTypeAndExpected.put( N5ConnectedComponentsSpark.NeighborhoodShapeType.Box, data ); // single component

        runTest( n5, shapeTypeAndExpected );
    }

    private void runTest( final N5Writer n5, final Map< N5ConnectedComponentsSpark.NeighborhoodShapeType, int[] > shapeTypeAndExpected ) throws IOException
    {
        for ( final Map.Entry< N5ConnectedComponentsSpark.NeighborhoodShapeType, int[] > shapeTypeAndExpectedEntry : shapeTypeAndExpected.entrySet() )
        {
            N5ConnectedComponentsSpark.connectedComponents(
                    sparkContext,
                    () -> new N5FSWriter( basePath ),
                    datasetPath,
                    relabeledDatasetPath,
                    shapeTypeAndExpectedEntry.getKey(),
                    OptionalDouble.empty(),
                    OptionalLong.empty() );

            Assert.assertTrue( n5.datasetExists( datasetPath ) );
            Assert.assertTrue( n5.datasetExists( relabeledDatasetPath ) );

            final RandomAccessibleInterval< UnsignedLongType > output = N5Utils.open( n5, relabeledDatasetPath );
            Assert.assertArrayEquals( shapeTypeAndExpectedEntry.getValue(), imgToArray( output ) );

            Assert.assertTrue( n5.remove( relabeledDatasetPath ) );
        }
    }

    private int[] imgToArray( final RandomAccessibleInterval< ? extends IntegerType< ? > > img )
    {
        final int[] data = new int[ ( int ) Intervals.numElements( img ) ];
        int index = 0;
        for ( final IntegerType< ? > val : Views.flatIterable( img ) )
            data[ index++ ] = val.getInteger();
        return data;
    }
}
