package org.janelia.saalfeldlab.n5.spark;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.util.unionfind.IntArrayRankedUnionFind;
import net.imglib2.algorithm.util.unionfind.LongHashMapUnionFind;
import net.imglib2.algorithm.util.unionfind.UnionFind;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.GridUtils;
import org.janelia.saalfeldlab.n5.spark.util.N5Compression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class N5ConnectedComponentsSpark
{
    private static final int MAX_PARTITIONS = 15000;

    public static < T extends IntegerType< T > & NativeType< T > > void connectedComponents(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String inputDatasetPath,
            final String outputDatasetPath,
            final Optional< int[] > blockSizeOptional,
            final Optional< Compression > compressionOptional ) throws IOException
    {
        final N5Writer n5 = n5Supplier.get();
        if ( n5.datasetExists( outputDatasetPath ) )
            throw new RuntimeException( "Output dataset already exists: " + outputDatasetPath );

        final String tempDatasetPath = outputDatasetPath + "-blockwise";
        if ( n5.datasetExists( tempDatasetPath ) )
            throw new RuntimeException( "Temporary dataset already exists: " + tempDatasetPath );

        final DatasetAttributes inputAttributes = n5.getDatasetAttributes( inputDatasetPath );
        final long[] dimensions = inputAttributes.getDimensions();
        final int[] inputBlockSize = inputAttributes.getBlockSize();
        final Compression inputCompression = inputAttributes.getCompression();
        final int[] outputBlockSize = blockSizeOptional.isPresent() ? blockSizeOptional.get() : inputBlockSize;
        final Compression outputCompression = compressionOptional.isPresent() ? compressionOptional.get() : inputCompression;

        n5.createDataset( tempDatasetPath, dimensions, outputBlockSize, DataType.UINT64, outputCompression );
        generateBlockwiseLabeling( sparkContext, n5Supplier, inputDatasetPath, tempDatasetPath );

        final UnionFind unionFind = findTouchingBlockwiseComponents( sparkContext, n5Supplier, tempDatasetPath );

        n5.createDataset( outputDatasetPath, dimensions, outputBlockSize, DataType.UINT64, outputCompression );
        relabelBlockwiseComponents( sparkContext, n5Supplier, tempDatasetPath, outputDatasetPath, unionFind );

        N5RemoveSpark.remove( sparkContext, n5Supplier, tempDatasetPath );
    }

    private static < T extends IntegerType< T > & NativeType< T > > void generateBlockwiseLabeling(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String inputDatasetPath,
            final String tempDatasetPath ) throws IOException
    {
        final DatasetAttributes outputDatasetAttributes = n5Supplier.get().getDatasetAttributes( tempDatasetPath );
        final long[] dimensions = outputDatasetAttributes.getDimensions();
        final int[] blockSize = outputDatasetAttributes.getBlockSize();

        final long numOutputBlocks = Intervals.numElements( new CellGrid( dimensions, blockSize ).getGridDimensions() );
        final List< Long > outputBlockIndexes = LongStream.range( 0, numOutputBlocks ).boxed().collect( Collectors.toList() );

        sparkContext.parallelize( outputBlockIndexes, Math.min( outputBlockIndexes.size(), MAX_PARTITIONS ) ).foreach( outputBlockIndex ->
        {
            final Interval outputBlockInterval = GridUtils.getCellInterval( new CellGrid( dimensions, blockSize ), outputBlockIndex );

            final N5Writer n5Local = n5Supplier.get();
            final RandomAccessibleInterval< T > input = N5Utils.open( n5Local, inputDatasetPath );
            final RandomAccessibleInterval< T > inputBlock = Views.interval( input, outputBlockInterval );

            final RandomAccessibleInterval< UnsignedLongType > outputBlock = Views.translate(
                    ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( outputBlockInterval ) ),
                    Intervals.minAsLongArray( outputBlockInterval ) );

            final RandomAccessibleInterval< BoolType > binaryInput = Converters.convert(
                    inputBlock,
                    ( in, out ) -> out.set( in.getIntegerLong() != 0 ),
                    new BoolType() );

            ConnectedComponentAnalysis.connectedComponents(
                    binaryInput,
                    outputBlock,
                    new DiamondShape( 1 ),
                    n -> new IntArrayRankedUnionFind( ( int ) n ),
                    ConnectedComponentAnalysis.idFromIntervalIndexer( outputBlockInterval ),
                    root -> root + 1 );

            N5Utils.saveNonEmptyBlock(
                    outputBlock,
                    n5Local,
                    tempDatasetPath,
                    new UnsignedLongType() );
        } );
    }

    private static UnionFind findTouchingBlockwiseComponents(
            final JavaSparkContext sparkContext,
            final N5ReaderSupplier n5Supplier,
            final String tempDatasetPath ) throws IOException
    {
        final DatasetAttributes outputDatasetAttributes = n5Supplier.get().getDatasetAttributes( tempDatasetPath );
        final long[] dimensions = outputDatasetAttributes.getDimensions();
        final int[] blockSize = outputDatasetAttributes.getBlockSize();

        final long numBlocks = Intervals.numElements( new CellGrid( dimensions, blockSize ).getGridDimensions() );
        final List< Long > blockIndexes = LongStream.range( 0, numBlocks ).boxed().collect( Collectors.toList() );

        final Set< List< Long > > touchingPairs = sparkContext
                .parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) )
                .map( outputBlockIndex ->
        {
            final Interval blockInterval = GridUtils.getCellInterval( new CellGrid( dimensions, blockSize ), outputBlockIndex );
            final N5Reader n5Local = n5Supplier.get();
            final RandomAccessibleInterval< UnsignedLongType > labeling = N5Utils.open( n5Local, tempDatasetPath );
            final RandomAccessibleInterval< UnsignedLongType > block = Views.interval( labeling, blockInterval );

            final Set< List< Long > > blockTouchingPairs = new HashSet<>();
            for ( int d = 0; d < block.numDimensions(); ++d )
            {
                // get the block interval and expand it by 1 px in all dimensions to be able to access the first planes of the next block
                final long[] expansion = new long[ block.numDimensions() ];
                expansion[ d ] = 1;
                final RandomAccessibleInterval< UnsignedLongType > expandedBlock = Views.expandZero( block, expansion );

                final Cursor< UnsignedLongType >[] planeCursors = new Cursor[ 2 ];
                for ( int i = 0; i < 2; ++i )
                {
                    final RandomAccessibleInterval< UnsignedLongType > plane = Views.hyperSlice(expandedBlock, d, blockInterval.max(d) + i);
                    planeCursors[ i ] = Views.flatIterable( plane ).cursor();
                }

                // find correspondences between touching ids in the current block and in the next block
                while ( planeCursors[ 0 ].hasNext() || planeCursors[ 1 ].hasNext() )
                {
                    final long x = planeCursors[ 0 ].next().getLong();
                    final long y = planeCursors[ 1 ].next().getLong();
                    if ( x != 0 && y != 0 )
                        blockTouchingPairs.add( Arrays.asList( x, y ) );
                }
            }
            return blockTouchingPairs;
        } )
                .treeReduce(
                        ( a, b ) -> { a.addAll( b ); return a; },
                        Integer.MAX_VALUE // max possible aggregation depth
        );

        // perform union find to merge all touching objects
        long elapsedMsec = System.currentTimeMillis();

        final UnionFind unionFind = new LongHashMapUnionFind();
        for ( final List< Long > touchingPair : touchingPairs )
            unionFind.join( touchingPair.get( 0 ), touchingPair.get( 1 ) );

        elapsedMsec = System.currentTimeMillis() - elapsedMsec;
        System.out.println( String.format(
                "Global union-find took %d seconds. Total number of components: %d",
                elapsedMsec / 1000,
                unionFind.setCount() ) );

        return unionFind;
    }

    private static void relabelBlockwiseComponents(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String tempDatasetPath,
            final String outputDatasetPath,
            final UnionFind unionFind ) throws IOException
    {
        final DatasetAttributes outputDatasetAttributes = n5Supplier.get().getDatasetAttributes( outputDatasetPath );
        final long[] dimensions = outputDatasetAttributes.getDimensions();
        final int[] blockSize = outputDatasetAttributes.getBlockSize();

        final long numBlocks = Intervals.numElements( new CellGrid( dimensions, blockSize ).getGridDimensions() );
        final List< Long > blockIndexes = LongStream.range( 0, numBlocks ).boxed().collect( Collectors.toList() );

        final Broadcast< UnionFind > unionFindBroadcast = sparkContext.broadcast( unionFind );

        sparkContext.parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) ).foreach( outputBlockIndex ->
        {
            final Interval blockInterval = GridUtils.getCellInterval( new CellGrid( dimensions, blockSize ), outputBlockIndex );
            final N5Writer n5Local = n5Supplier.get();
            final UnionFind unionFindLocal = unionFindBroadcast.getValue();
            final RandomAccessibleInterval<UnsignedLongType> input = N5Utils.open( n5Local, tempDatasetPath );
            final RandomAccessibleInterval<UnsignedLongType> inputBlock = Views.interval( input, blockInterval );

            final RandomAccessibleInterval< UnsignedLongType > outputBlock = Views.translate(
                    ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( blockInterval ) ),
                    Intervals.minAsLongArray( blockInterval ) );

            final Cursor< UnsignedLongType > inputCursor = Views.flatIterable( inputBlock ).cursor();
            final Cursor< UnsignedLongType > outputCursor = Views.flatIterable( outputBlock ).cursor();
            while ( inputCursor.hasNext() || outputCursor.hasNext() )
            {
                final long inputId = inputCursor.next().get();
                final UnsignedLongType outputVal = outputCursor.next();
                if ( inputId != 0 )
                    outputVal.set( unionFindLocal.findRoot( inputId ) );
            }

            N5Utils.saveNonEmptyBlock(
                    outputBlock,
                    n5Local,
                    outputDatasetPath,
                    new UnsignedLongType() );
        } );

        unionFindBroadcast.destroy();
    }

    public static void main( final String... args ) throws IOException
    {
        final Arguments parsedArgs = new Arguments( args );
        if ( !parsedArgs.parsedSuccessfully() )
            System.exit( 1 );

        try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
                .setAppName( "N5ConnectedComponentsSpark" )
                .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
        ) )
        {
            connectedComponents(
                    sparkContext,
                    () -> new N5FSWriter( parsedArgs.getN5Path() ),
                    parsedArgs.getInputDatasetPath(),
                    parsedArgs.getOutputDatasetPath(),
                    Optional.ofNullable( parsedArgs.getBlockSize() ),
                    Optional.ofNullable( parsedArgs.getCompression() )
            );
        }

        System.out.println( System.lineSeparator() + "Done" );
    }

    private static class Arguments implements Serializable
    {
        private static final long serialVersionUID = 4847292347478989514L;

        @Option(name = "-n", aliases = { "-n", "--n5Path" }, required = true,
                usage = "Path to the N5 container.")
        private String n5Path;

        @Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
                usage = "Path to the input binary mask dataset within the N5 container (e.g. data/group/s0).")
        private String inputDatasetPath;

        @Option(name = "-o", aliases = { "--outputDatasetPath" }, required = true,
                usage = "Output dataset path.")
        private String outputDatasetPath;

        @Option(name = "-b", aliases = { "--blockSize" }, required = false,
                usage = "Block size for the output dataset (same as input dataset block size by default).")
        private String blockSizeStr;

        @Option(name = "-c", aliases = { "--compression" }, required = false,
                usage = "Compression to be used for the converted dataset (same as input dataset compression by default).")
        private N5Compression n5Compression;

        private int[] blockSize;
        private boolean parsedSuccessfully = false;

        public Arguments( final String... args )
        {
            final CmdLineParser parser = new CmdLineParser( this );
            try
            {
                parser.parseArgument( args );
                blockSize = blockSizeStr != null ? CmdUtils.parseIntArray( blockSizeStr ) : null;
                parsedSuccessfully = true;
            }
            catch ( final CmdLineException e )
            {
                System.err.println( e.getMessage() );
                parser.printUsage( System.err );
            }
        }

        public boolean parsedSuccessfully() { return parsedSuccessfully; }
        public String getN5Path() { return n5Path; }
        public String getInputDatasetPath() { return inputDatasetPath; }
        public String getOutputDatasetPath() { return outputDatasetPath; }
        public int[] getBlockSize() { return blockSize; }
        public Compression getCompression() { return n5Compression != null ? n5Compression.get() : null; }
    }
}
