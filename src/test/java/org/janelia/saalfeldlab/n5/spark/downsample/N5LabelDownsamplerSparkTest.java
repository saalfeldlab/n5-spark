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
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
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

public class N5LabelDownsamplerSparkTest extends AbstractN5SparkTest
{
	static private final String datasetPath = "data";
	static private final String downsampledDatasetPath = "downsampled-data";

	@Test
	public void testLabelDownsampling() throws IOException
	{
		final N5Writer n5 = new N5FSWriter( basePath );

		N5Utils.save(
				ArrayImgs.unsignedLongs(
						new long[] {
								5, 4, 8, 8,
								5, 5, 5, 8,
								5, 6, 6, 6,
								1, 2, 3, 4,

								9, 9, 8, 8,
								7, 5, 8, 8,
								5, 5, 6, 8,
								5, 5, 8, 8
							},
						new long[] { 4, 4, 2 }
					),
				n5,
				datasetPath,
				new int[] { 2, 2, 1 },
				new GzipCompression()
			);

		N5LabelDownsamplerSpark.downsampleLabel(
				sparkContext,
				() -> new N5FSWriter( basePath ),
				datasetPath,
				downsampledDatasetPath,
				new int[] { 2, 2, 2 }
			);
		final DatasetAttributes downsampledAttributes = n5.getDatasetAttributes( downsampledDatasetPath );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, downsampledAttributes.getDimensions() );
		Assert.assertArrayEquals( new int[] { 2, 2, 1 }, downsampledAttributes.getBlockSize() );
		Assert.assertArrayEquals( new long[] { 5, 8, 5, 6 }, getArrayFromRandomAccessibleInterval( N5Utils.open( n5, downsampledDatasetPath ) ) );
	}

	private long[] getArrayFromRandomAccessibleInterval( final RandomAccessibleInterval< UnsignedLongType > rai )
	{
		final long[] arr = new long[ ( int ) Intervals.numElements( rai ) ];
		final Cursor< UnsignedLongType > cursor = Views.flatIterable( rai ).cursor();
		int i = 0;
		while ( cursor.hasNext() )
			arr[ i++ ] = cursor.next().get();
		return arr;
	}
}
