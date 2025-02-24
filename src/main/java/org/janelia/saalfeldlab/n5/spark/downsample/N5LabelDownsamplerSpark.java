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
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodFactory;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodUnsafe;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.algorithm.util.Singleton;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class N5LabelDownsamplerSpark {
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Downsamples the given input dataset with respect to the given downsampling factors.
	 * Instead of averaging, it uses the value that is the most frequent in the neighborhood.
	 * In case of equal frequencies, the smallest label value among them is used.
	 * The output dataset will be created within the same N5 container with the same block size as the input dataset.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @throws IOException
	 */
	public static <T extends NativeType<T> & IntegerType<T>> void downsampleLabel(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors) throws IOException {

		downsampleLabel(
				sparkContext,
				n5Supplier,
				inputDatasetPath,
				outputDatasetPath,
				downsamplingFactors,
				null
		);
	}

	/**
	 * Downsamples the given input dataset with respect to the given downsampling factors.
	 * Instead of averaging, it uses the value that is the most frequent in the neighborhood.
	 * In case of equal frequencies, the smallest label value among them is used.
	 * The output dataset will be created within the same N5 container with given block size.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @param blockSize
	 * @throws IOException
	 */
	public static <T extends NativeType<T> & IntegerType<T>> void downsampleLabel(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors,
			final int[] blockSize) throws IOException {

		downsampleLabel(
				sparkContext,
				n5Supplier,
				inputDatasetPath,
				outputDatasetPath,
				downsamplingFactors,
				blockSize,
				false
		);
	}

	/**
	 * Downsamples the given input dataset with respect to the given downsampling factors.
	 * Instead of averaging, it uses the value that is the most frequent in the neighborhood.
	 * In case of equal frequencies, the smallest label value among them is used.
	 * The output dataset will be created within the same N5 container with given block size.
	 *
	 * @param sparkContext
	 * @param n5Supplier
	 * @param inputDatasetPath
	 * @param outputDatasetPath
	 * @param downsamplingFactors
	 * @param blockSize
	 * @throws IOException
	 */
	public static <T extends NativeType<T> & IntegerType<T>> void downsampleLabel(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String inputDatasetPath,
			final String outputDatasetPath,
			final int[] downsamplingFactors,
			final int[] blockSize,
			final boolean overwriteExisting) throws IOException {

		final N5Writer n5 = n5Supplier.get();
		if (!n5.datasetExists(inputDatasetPath))
			throw new IllegalArgumentException("Input N5 dataset " + inputDatasetPath + " does not exist");

		final DatasetAttributes inputAttributes = n5.getDatasetAttributes(inputDatasetPath);
		final int dim = inputAttributes.getNumDimensions();

		if (dim != downsamplingFactors.length)
			throw new IllegalArgumentException("Downsampling parameters do not match data dimensionality.");

		final long[] outputDimensions = new long[dim];
		for (int d = 0; d < dim; ++d)
			outputDimensions[d] = inputAttributes.getDimensions()[d] / downsamplingFactors[d];

		if (Arrays.stream(outputDimensions).min().getAsLong() < 1)
			throw new IllegalArgumentException("Degenerate output dimensions: " + Arrays.toString(outputDimensions));

		final int[] outputBlockSize = blockSize != null ? blockSize : inputAttributes.getBlockSize();

		if (n5.datasetExists(outputDatasetPath)) {
			if (!overwriteExisting) {
				throw new RuntimeException("Output dataset already exists: " + outputDatasetPath);
			} else {
				// Requested to overwrite an existing dataset, make sure that the block sizes match
				final int[] oldOutputBlockSize = n5.getDatasetAttributes(outputDatasetPath).getBlockSize();
				if (!Arrays.equals(outputBlockSize, oldOutputBlockSize))
					throw new RuntimeException("Cannot overwrite existing dataset if the block sizes are not the same.");
			}
		}

		n5.createDataset(
				outputDatasetPath,
				outputDimensions,
				outputBlockSize,
				inputAttributes.getDataType(),
				inputAttributes.getCompression()
		);

		final CellGrid outputCellGrid = new CellGrid(outputDimensions, outputBlockSize);
		final long numDownsampledBlocks = Intervals.numElements(outputCellGrid.getGridDimensions());
		final List<Long> blockIndexes = LongStream.range(0, numDownsampledBlocks).boxed().collect(Collectors.toList());

		sparkContext
				.parallelize(blockIndexes, Math.min(blockIndexes.size(), MAX_PARTITIONS))
				.foreach(blockIndex -> {
					final CellGrid cellGrid = new CellGrid(outputDimensions, outputBlockSize);
					final long[] blockGridPosition = new long[cellGrid.numDimensions()];
					cellGrid.getCellGridPositionFlat(blockIndex, blockGridPosition);

					final long[] sourceMin = new long[dim], sourceMax = new long[dim], targetMin = new long[dim], targetMax = new long[dim];
					final int[] cellDimensions = new int[dim];
					cellGrid.getCellDimensions(blockGridPosition, targetMin, cellDimensions);
					for (int d = 0; d < dim; ++d) {
						targetMax[d] = targetMin[d] + cellDimensions[d] - 1;
						sourceMin[d] = targetMin[d] * downsamplingFactors[d];
						sourceMax[d] = targetMax[d] * downsamplingFactors[d] + downsamplingFactors[d] - 1;
					}
					final Interval sourceInterval = new FinalInterval(sourceMin, sourceMax);
					final Interval targetInterval = new FinalInterval(targetMin, targetMax);

					final N5Writer n5Local = n5Supplier.get();
					final String writerCacheKey = new URIBuilder(n5Local.getURI())
							.setParameters(
									new BasicNameValuePair("type", "writer"),
									new BasicNameValuePair("call", "n5-downsample-spark")
							).toString();

					final N5Writer n5Writer = Singleton.get(writerCacheKey, () -> n5Local);

					final String imgCacheKey = new URIBuilder(n5Writer.getURI())
							.setParameters(
									new BasicNameValuePair("type", "writer"),
									new BasicNameValuePair("dataset", inputDatasetPath),
									new BasicNameValuePair("call", "n5-downsample-spark")
							).toString();

					final RandomAccessibleInterval< T > source = Singleton.get(imgCacheKey, () -> N5Utils.open( n5Writer, inputDatasetPath ));
					final RandomAccessibleInterval<T> sourceBlock = Views.offsetInterval(source, sourceInterval);

					/* test if empty */
					final T defaultValue = Util.getTypeFromInterval(sourceBlock).createVariable();
					boolean isEmpty = true;
					for (final T t : Views.iterable(sourceBlock)) {
						isEmpty &= defaultValue.valueEquals(t);
						if (!isEmpty)
							break;
					}
					if (isEmpty)
						return;

					/* do if not empty */
					final RandomAccessibleInterval<T> targetBlock = new ArrayImgFactory<>(defaultValue).create(targetInterval);
					downsampleLabel(sourceBlock, targetBlock, downsamplingFactors);

					if (overwriteExisting) {
						// Empty blocks will not be written out. Delete blocks to avoid remnant blocks if overwriting.
						N5Utils.deleteBlock(targetBlock, n5Writer, outputDatasetPath, blockGridPosition);
					}

					N5Utils.saveNonEmptyBlock(targetBlock, n5Writer, outputDatasetPath, blockGridPosition, defaultValue);
				});
	}

	/**
	 * Based on {@link bdv.export.Downsample}.
	 */
	private static <T extends IntegerType<T>> void downsampleLabel(
			final RandomAccessible<T> input,
			final RandomAccessibleInterval<T> output,
			final int[] factor) {

		assert input.numDimensions() == output.numDimensions();
		assert input.numDimensions() == factor.length;

		final int n = input.numDimensions();
		final RectangleNeighborhoodFactory<T> f = RectangleNeighborhoodUnsafe.<T>factory();
		final long[] dim = new long[n];
		for (int d = 0; d < n; ++d)
			dim[d] = factor[d];
		final Interval spanInterval = new FinalInterval(dim);

		final long[] minRequiredInput = new long[n];
		final long[] maxRequiredInput = new long[n];
		output.min(minRequiredInput);
		output.max(maxRequiredInput);
		for (int d = 0; d < n; ++d) {
			minRequiredInput[d] *= factor[d];
			maxRequiredInput[d] *= factor[d];
			maxRequiredInput[d] += factor[d] - 1;
		}
		final RandomAccessibleInterval<T> requiredInput = Views.interval(input, new FinalInterval(minRequiredInput, maxRequiredInput));

		final RectangleShape.NeighborhoodsAccessible<T> neighborhoods = new RectangleShape.NeighborhoodsAccessible<>(requiredInput, spanInterval, f);
		final RandomAccess<Neighborhood<T>> block = neighborhoods.randomAccess();

		final Cursor<T> out = Views.iterable(output).localizingCursor();

		final Map<Long, Integer> labelCount = new HashMap<>();

		while (out.hasNext()) {
			final T o = out.next();
			for (int d = 0; d < n; ++d)
				block.setPosition(out.getLongPosition(d) * factor[d], d);

			// find count for each label
			labelCount.clear();
			for (final T i : block.get())
				labelCount.put(i.getIntegerLong(), labelCount.getOrDefault(i.getIntegerLong(), 0) + 1);

			// find the most frequent label, or in case of multiple labels with the same frequency, the smallest label among them
			int maxCount = Integer.MIN_VALUE;
			long labelWithMaxCount = Long.MIN_VALUE;
			for (final Entry<Long, Integer> entry : labelCount.entrySet()) {
				if (maxCount < entry.getValue()) {
					maxCount = entry.getValue();
					labelWithMaxCount = entry.getKey();
				} else if (maxCount == entry.getValue() && labelWithMaxCount > entry.getKey()) {
					labelWithMaxCount = entry.getKey();
				}
			}

			o.setInteger(labelWithMaxCount);
		}
	}

	public static void main(final String... args) throws IOException, CmdLineException {

		final Arguments parsedArgs = new Arguments(args);

		try (final JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
				.setAppName("N5LabelDownsamplerSpark")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		)) {
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter(parsedArgs.getN5Path());
			downsampleLabel(
					sparkContext,
					n5Supplier,
					parsedArgs.getInputDatasetPath(),
					parsedArgs.getOutputDatasetPath(),
					parsedArgs.getDownsamplingFactors(),
					parsedArgs.getBlockSize()
			);
		}
		System.out.println("Done");
	}

	private static class Arguments implements Serializable {
		private static final long serialVersionUID = -1467734459169624759L;

		@Option(name = "-n", aliases = {"--n5Path"}, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = {"--inputDatasetPath"}, required = true,
				usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		@Option(name = "-o", aliases = {"--outputDatasetPath"}, required = true,
				usage = "Path to the output dataset to be created (e.g. data/group/s1).")
		private String outputDatasetPath;

		@Option(name = "-f", aliases = {"--factors"}, required = true,
				usage = "Downsampling factors.")
		private String downsamplingFactors;

		@Option(name = "-b", aliases = {"--blockSize"}, required = false,
				usage = "Block size for the output dataset (by default same as for input dataset).")
		private String blockSize;

		public Arguments(final String... args) throws IllegalArgumentException {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
				System.exit(1);
			}
		}

		public String getN5Path() {

			return n5Path;
		}

		public String getInputDatasetPath() {

			return inputDatasetPath;
		}

		public String getOutputDatasetPath() {

			return outputDatasetPath;
		}

		public int[] getDownsamplingFactors() {

			return CmdUtils.parseIntArray(downsamplingFactors);
		}

		public int[] getBlockSize() {

			return CmdUtils.parseIntArray(blockSize);
		}
	}
}
