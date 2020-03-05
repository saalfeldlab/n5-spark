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
