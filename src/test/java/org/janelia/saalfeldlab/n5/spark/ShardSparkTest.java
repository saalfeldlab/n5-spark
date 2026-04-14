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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import net.imglib2.iterator.IntervalIterator;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

/**
 * Benchmark that tests parallel writes to a sharded Zarr v3 dataset using Apache Spark.
 * <p>
 * Creates a 2D dataset backed by a single shard and distributes writes across Spark tasks,
 * one task per chunk row. Each task calls {@code writeRegion} with a supplier that only
 * provides blocks for its assigned row, exercising concurrent shard-merge correctness.
 * After each row is written, a validation step reads back all chunk positions and confirms
 * that exactly one row's worth of blocks is present and contains the expected values.
 * <p>
 * The dataset root, shape, shard size, and chunk size are all configurable via command-line
 * arguments ({@code --root}, {@code --shape}, {@code --shard-size}, {@code --chunk-size}),
 * with defaults of a 512×512 dataset, 512×512 shard, and 32×32 chunks.
 */
public class ShardSparkTest {

	private static final int MAX_PARTITIONS = 15000;
	static final String DSET = "";

	String root;
	long[] shape = new long[]{512, 512};
	int[] shardSize = {512, 512};
	int[] chunkSize = {32, 32};
	int[] chunksPerDataset;
	int chunkN;
	ArrayList<long[]> positions;

	void initDerived() {
		chunksPerDataset = new int[shardSize.length];
		for (int i = 0; i < shardSize.length; i++)
			chunksPerDataset[i] = shardSize[i] / chunkSize[i];

		chunkN = 1;
		for (int s : chunkSize) chunkN *= s;

		positions = new ArrayList<>();
		final long[] dims = new long[chunksPerDataset.length];
		for (int i = 0; i < dims.length; i++) dims[i] = chunksPerDataset[i];
		final IntervalIterator it = new IntervalIterator(dims);
		while (it.hasNext()) {
			it.fwd();
			positions.add(it.positionAsLongArray().clone());
		}
	}

	static long[] parseLongArray(final String s) {
		return Arrays.stream(s.split(",")).mapToLong(Long::parseLong).toArray();
	}

	static int[] parseIntArray(final String s) {
		return Arrays.stream(s.split(",")).mapToInt(Integer::parseInt).toArray();
	}

	public static void main(final String... args) throws IOException {

		final ShardSparkTest test = new ShardSparkTest();

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "--root":       test.root      = args[++i]; break;
				case "--shape":      test.shape     = parseLongArray(args[++i]); break;
				case "--shard-size": test.shardSize = parseIntArray(args[++i]); break;
				case "--chunk-size": test.chunkSize = parseIntArray(args[++i]); break;
				default: System.err.println("Unknown argument: " + args[i]);
			}
		}
		test.initDerived();
		if( test.root == null) {
			System.err.println("Root must be provided.");
			return;
		}

		final String root = test.root;
		final N5WriterSupplier supplier = new N5WriterSupplier() {
			@Override
			public N5Writer get() throws IOException {
				return new N5Factory().openWriter(root);
			}
		};

		try (final JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
				.setAppName("SparkShardTest")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))) {
			test.run(sparkContext, supplier, DSET);
		}
	}

	public ShardSparkTest() {

	}

	public < I extends NativeType< I > & RealType< I > > void run(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath) throws IOException
	{
		final N5Writer n5 = n5OutputSupplier.get();

		ZarrV3DatasetAttributes tmpAttrs = ZarrV3DatasetAttributes.builder(shape, DataType.INT32)
			.shardShape(shardSize)
			.blockSize(chunkSize)
			.compression(new ZstandardCompression())
			.build();

		n5.remove(outputDatasetPath);
		n5.createDataset(outputDatasetPath, tmpAttrs);
		writeChunksParallel(sparkContext, n5OutputSupplier, outputDatasetPath);
	}

	public < I extends NativeType< I > & RealType< I > > void writeChunksParallel(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5OutputSupplier,
			final String outputDatasetPath) throws IOException
	{
		final List< Integer > outputBlockIndexes = IntStream.range( 0, chunksPerDataset[0] ).boxed().collect( Collectors.toList() );
		final int[] chunkSize = this.chunkSize;
		final int chunkN = this.chunkN;
		final ArrayList<long[]> positions = this.positions;
		final int chunksPerDim0 = this.chunksPerDataset[0];

		sparkContext.parallelize( outputBlockIndexes, Math.min( outputBlockIndexes.size(), MAX_PARTITIONS ) ).foreach( i -> {

			final N5Writer n5 = n5OutputSupplier.get();
			final DatasetAttributes attrs = n5.getDatasetAttributes( outputDatasetPath );

			final long[] min = new long[]{0, 0};
			final long[] dimensions = attrs.getDimensions();

			n5.writeRegion(outputDatasetPath, attrs, min, dimensions, blockSupplierRow(i, chunkSize, chunkN), true);
			validate(n5, outputDatasetPath, attrs, positions, chunksPerDim0);

		});
	}

	static DataBlockSupplier<int[]> blockSupplierRow(final int i, final int[] chunkSize, final int chunkN) {

		return new DataBlockSupplier<int[]>() {

			@Override
			public DataBlock<int[]> get(long[] gridPos, DataBlock<int[]> existingDataBlock) {

				if (gridPos[0] == i)
					return new IntArrayDataBlock(chunkSize, gridPos, data(chunkN, (int)(gridPos[0] + gridPos[1])));
				else
					return null;
			}
		};
	}

	static boolean validateBlockRow(final DataBlock<int[]> block) {

		// assume other methods ensure that the null blocks are correctly null
		if (block == null)
			return true;

		final long[] position = block.getGridPosition();
		return Arrays.stream(block.getData()).anyMatch(v -> v == position[0] + position[1]);
	}

	static int[] data(final int chunkN, final int value) {
		final int[] data = new int[chunkN];
		Arrays.fill(data, value);
		return data;
	}

	static boolean validate(
			final N5Writer n5,
			final String outputDatasetPath,
			final DatasetAttributes attributes,
			final ArrayList<long[]> positions,
			final int chunksPerDim0) throws IOException {

		final List<DataBlock<int[]>> allChunks = n5.readChunks(outputDatasetPath, attributes, positions);
		HashSet<Integer> uniqueRows = new HashSet<>();
		allChunks.stream().forEach(b -> {
			if (b != null) {
				uniqueRows.add((int)b.getGridPosition()[0]);
			}
		});

		if (uniqueRows.size() == 0) {
			System.out.println("No blocks found");
			return false;
		} else if (uniqueRows.size() > 1) {
			System.out.println("Blocks from multiple rows found: " + uniqueRows);
			return false;
		}

		int numBlocks = 0;
		for (int i = 0; i < allChunks.size(); i++) {
			final DataBlock<int[]> blk = allChunks.get(i);
			if (blk != null) {
				numBlocks++;
				if (!validateBlockRow(blk)) {
					System.out.println("block invalid at " + Arrays.toString(blk.getGridPosition()));
					return false;
				}
			}
		}

		if (numBlocks != chunksPerDim0) {
			System.out.println("wrong number of blocks: " + numBlocks);
			return false;
		}

		return true;
	}

}
