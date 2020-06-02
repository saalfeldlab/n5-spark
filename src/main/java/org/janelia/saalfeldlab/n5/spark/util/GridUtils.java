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
package org.janelia.saalfeldlab.n5.spark.util;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.img.cell.CellGrid;

import java.util.Arrays;

public class GridUtils
{
    public static Interval getCellInterval(final CellGrid grid, final long cellIndex)
    {
        final long[] cellMin = new long[grid.numDimensions()], cellMax = new long[grid.numDimensions()];
        final int[] cellDims = new int[grid.numDimensions()];
        grid.getCellDimensions(cellIndex, cellMin, cellDims);
        Arrays.setAll(cellMax, d -> cellMin[d] + cellDims[d] - 1);
        return new FinalInterval(cellMin, cellMax);
    }

    public static Interval getCellInterval(final CellGrid grid, final long[] cellPosition)
    {
        final long[] cellMin = new long[grid.numDimensions()], cellMax = new long[grid.numDimensions()];
        final int[] cellDims = new int[grid.numDimensions()];
        grid.getCellDimensions(cellPosition, cellMin, cellDims);
        Arrays.setAll(cellMax, d -> cellMin[d] + cellDims[d] - 1);
        return new FinalInterval(cellMin, cellMax);
    }
}
