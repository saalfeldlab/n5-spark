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
package org.janelia.saalfeldlab.n5.spark.downsample;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.AbstractN5SparkTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class N5OffsetDownsamplerSparkTest extends AbstractN5SparkTest
{
	static private final String datasetPath = "data";
	static private final String downsampledDatasetPath = "downsampled-data";

	@Test
	public void testDownsamplingWithPositiveOffset1D() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		for ( int blockSize = 1; blockSize <= 10; ++blockSize )
		{
			createDataset( n5, new long[] { 9 }, new int[] { blockSize } );

			N5OffsetDownsamplerSpark.downsampleWithOffset(
					sparkContext,
					() -> new N5FSWriter( basePath ),
					datasetPath,
					downsampledDatasetPath,
					new int[] { 4 },
					new long[] { 3 }
				);

			final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
			Assert.assertArrayEquals( new long[] { 3 }, downsampledAttributes.getDimensions() );
			Assert.assertArrayEquals( new int[] { blockSize }, downsampledAttributes.getBlockSize() );

			Assert.assertArrayEquals(
					new int[] {
							1,
							( int ) Util.round( ( 2 + 3 + 4 + 5 ) / 4. ),
							( int ) Util.round( ( 6 + 7 + 8 + 9 ) / 4. )
						},
					getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
				);

			Assert.assertTrue( n5.remove( downsampledDatasetPath ) );
		}
	}

	@Test
	public void testDownsamplingWithNegativeOffset1D() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		for ( int blockSize = 1; blockSize <= 10; ++blockSize )
		{
			createDataset( n5, new long[] { 10 }, new int[] { blockSize } );

			N5OffsetDownsamplerSpark.downsampleWithOffset(
					sparkContext,
					() -> new N5FSWriter( basePath ),
					datasetPath,
					downsampledDatasetPath,
					new int[] { 3 },
					new long[] { -1 }
				);

			final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
			Assert.assertArrayEquals( new long[] { 3 }, downsampledAttributes.getDimensions() );
			Assert.assertArrayEquals( new int[] { blockSize }, downsampledAttributes.getBlockSize() );

			Assert.assertArrayEquals(
					new int[] {
							( int ) Util.round( ( 2 + 3 + 4  ) / 3. ),
							( int ) Util.round( ( 5 + 6 + 7  ) / 3. ),
							( int ) Util.round( ( 8 + 9 + 10 ) / 3. )
						},
					getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
				);

			Assert.assertTrue( n5.remove( downsampledDatasetPath ) );
		}
	}

	@Test
	public void testDownsamplingWithOffset2D() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );
		for ( int blockSizeX = 1; blockSizeX <= 6; ++blockSizeX )
		{
			for ( int blockSizeY = 1; blockSizeY <= 8; ++blockSizeY )
			{
				createDataset( n5, new long[] { 5, 7 }, new int[] { blockSizeX, blockSizeY } );

				N5OffsetDownsamplerSpark.downsampleWithOffset(
						sparkContext,
						() -> new N5FSWriter( basePath ),
						datasetPath,
						downsampledDatasetPath,
						new int[] { 3, 3 },
						new long[] { 2, -2 }
					);

				final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
				Assert.assertArrayEquals( new long[] { 2, 1 }, downsampledAttributes.getDimensions() );
				Assert.assertArrayEquals( new int[] { blockSizeX, blockSizeY }, downsampledAttributes.getBlockSize() );

				Assert.assertArrayEquals(
						new int[] {
								( int ) Util.round( ( 11 + 16 + 21 ) / 3. ),
								( int ) Util.round( ( 12 + 13 + 14 + 17 + 18 + 19 + 22 + 23 + 24 ) / 9. ),
							},
						getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) )
					);

				Assert.assertTrue( n5.remove( downsampledDatasetPath ) );
			}
		}
	}


	private void createDataset( final N5Writer n5, final long[] dimensions, final int[] blockSize ) throws IOException
	{
		final int[] data = new int[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < data.length; ++i )
			data[ i ] = i + 1;
		N5Utils.save( ArrayImgs.ints( data, dimensions ), n5, datasetPath, blockSize, new GzipCompression() );
	}

	private int[] getArrayFromRandomAccessibleInterval( final RandomAccessibleInterval< IntType > rai )
	{
		final int[] arr = new int[ ( int ) Intervals.numElements( rai ) ];
		final Cursor< IntType > cursor = Views.flatIterable( rai ).cursor();
		int i = 0;
		while ( cursor.hasNext() )
			arr[ i++ ] = cursor.next().get();
		return arr;
	}
}
