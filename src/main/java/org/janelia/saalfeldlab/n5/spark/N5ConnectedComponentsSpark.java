package org.janelia.saalfeldlab.n5.spark;

import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.algorithm.neighborhood.Shape;
import net.imglib2.algorithm.util.unionfind.LongHashMapUnionFind;
import net.imglib2.algorithm.util.unionfind.UnionFind;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.RealType;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class N5ConnectedComponentsSpark
{
    private static final int MAX_PARTITIONS = 15000;

    public enum NeighborhoodShapeType
    {
        /**
         Only direct neighbors are considered (4-neighborhood in 2D, 6-neighborhood in 3D).
         */
        Diamond,

        /**
         Diagonal neighbors are considered too (8-neighborhood in 2D, 26-neighborhood in 3D).
         */
        Box
    }

    public static < T extends RealType< T > & NativeType< T > > void connectedComponents(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String inputDatasetPath,
            final String outputDatasetPath,
            final NeighborhoodShapeType neighborhoodShapeType,
            final Optional< Double > thresholdOptional,
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

        generateBlockwiseLabeling(
                sparkContext,
                n5Supplier,
                inputDatasetPath,
                tempDatasetPath,
                neighborhoodShapeType,
                thresholdOptional );

        final TLongLongHashMap parentsMap = findTouchingBlockwiseComponents(
                sparkContext,
                n5Supplier,
                tempDatasetPath,
                neighborhoodShapeType );

        n5.createDataset( outputDatasetPath, dimensions, outputBlockSize, DataType.UINT64, outputCompression );

        relabelBlockwiseComponents(
                sparkContext,
                n5Supplier,
                tempDatasetPath,
                outputDatasetPath,
                parentsMap );

        N5RemoveSpark.remove( sparkContext, n5Supplier, tempDatasetPath );
    }

    private static < T extends RealType< T > & NativeType< T > > void generateBlockwiseLabeling(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String inputDatasetPath,
            final String tempDatasetPath,
            final NeighborhoodShapeType neighborhoodShapeType,
            final Optional< Double > thresholdOptional ) throws IOException
    {
        final DatasetAttributes outputDatasetAttributes = n5Supplier.get().getDatasetAttributes( tempDatasetPath );
        final long[] dimensions = outputDatasetAttributes.getDimensions();
        final int[] blockSize = outputDatasetAttributes.getBlockSize();

        final double threshold = thresholdOptional.isPresent() ? thresholdOptional.get() : 0;

        final long numOutputBlocks = Intervals.numElements( new CellGrid( dimensions, blockSize ).getGridDimensions() );
        final List< Long > outputBlockIndexes = LongStream.range( 0, numOutputBlocks ).boxed().collect( Collectors.toList() );

        sparkContext.parallelize( outputBlockIndexes, Math.min( outputBlockIndexes.size(), MAX_PARTITIONS ) ).foreach( outputBlockIndex ->
        {
            final Interval outputBlockInterval = GridUtils.getCellInterval( new CellGrid( dimensions, blockSize ), outputBlockIndex );

            final N5Writer n5Local = n5Supplier.get();
            final RandomAccessibleInterval< T > input = N5Utils.open( n5Local, inputDatasetPath );
            final RandomAccessibleInterval< T > inputBlock = Views.interval( input, outputBlockInterval );

            final RandomAccessibleInterval< BoolType > binaryInput;
            if ( threshold != 0 )
            {
                binaryInput = Converters.convert(
                    inputBlock,
                    ( in, out ) -> out.set( in.getRealDouble() >= threshold ),
                    new BoolType() );
            }
            else
            {
                binaryInput = Converters.convert(
                        inputBlock,
                        ( in, out ) -> out.set( in.getRealDouble() > 0 ),
                        new BoolType() );
            }

            boolean isEmpty = true;
            for ( final Iterator< BoolType > it = Views.iterable( binaryInput ).iterator(); it.hasNext() && isEmpty; )
                isEmpty &= !it.next().get();
            if ( isEmpty )
                return;

            final RandomAccessibleInterval< UnsignedLongType > outputBlock = Views.translate(
                    ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( outputBlockInterval ) ),
                    Intervals.minAsLongArray( outputBlockInterval ) );

            final Shape neighborhoodShape;
            switch (neighborhoodShapeType)
            {
                case Diamond:
                    neighborhoodShape = new DiamondShape( 1 );
                    break;
                case Box:
                    neighborhoodShape = new RectangleShape( 1, true );
                    break;
                default:
                    throw new IllegalArgumentException( "Unknown or null neighborhood shape type: " + neighborhoodShapeType );
            }

            ConnectedComponentAnalysis.connectedComponents(
                    binaryInput,
                    outputBlock,
                    neighborhoodShape,
                    n -> new LongHashMapUnionFind(),
                    ConnectedComponentAnalysis.idFromIntervalIndexer( input ),
                    root -> root + 1 );

            N5Utils.saveNonEmptyBlock(
                    outputBlock,
                    n5Local,
                    tempDatasetPath,
                    new UnsignedLongType() );
        } );
    }

    private static TLongLongHashMap findTouchingBlockwiseComponents(
            final JavaSparkContext sparkContext,
            final N5ReaderSupplier n5Supplier,
            final String tempDatasetPath,
            final NeighborhoodShapeType neighborhoodShapeType ) throws IOException
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
            final RandomAccessibleInterval< UnsignedLongType > labeling = N5Utils.open( n5Supplier.get(), tempDatasetPath );

            final Set< List< Long > > blockTouchingPairs = new HashSet<>();
            for ( int d = 0; d < blockInterval.numDimensions(); ++d )
            {
                if ( blockInterval.max( d ) >= labeling.max( d ) )
                    continue;

                if ( neighborhoodShapeType == NeighborhoodShapeType.Diamond )
                {
                    // test the last plane of the current block against the first plane of the next block
                    final Cursor< UnsignedLongType >[] planeCursors = new Cursor[ 2 ];
                    for ( int i = 0; i < 2; ++i )
                    {
                        final long[] sliceMin = Intervals.minAsLongArray( blockInterval ), sliceMax = Intervals.maxAsLongArray( blockInterval );
                        sliceMin[ d ] = sliceMax[ d ] = blockInterval.max( d ) + i;
                        final RandomAccessibleInterval< UnsignedLongType > plane = Views.interval( labeling, sliceMin, sliceMax );
                        planeCursors[ i ] = Views.flatIterable( plane ).cursor();
                    }

                    while ( planeCursors[ 0 ].hasNext() || planeCursors[ 1 ].hasNext() )
                    {
                        final long x = planeCursors[ 0 ].next().getLong();
                        final long y = planeCursors[ 1 ].next().getLong();
                        if ( x != 0 && y != 0 )
                            blockTouchingPairs.add( Arrays.asList( x, y ) );
                    }
                }
                else if ( neighborhoodShapeType == NeighborhoodShapeType.Box )
                {
                    // test the last plane of the current block against the rectangular neighborhood of the next blocks
                    final long[] sliceMin = Intervals.minAsLongArray( blockInterval ), sliceMax = Intervals.maxAsLongArray( blockInterval );
                    sliceMin[ d ] = sliceMax[ d ] = blockInterval.max( d );
                    final RandomAccessibleInterval< UnsignedLongType > plane = Views.interval( labeling, sliceMin, sliceMax );
                    final Cursor< UnsignedLongType > planeCursor = Views.flatIterable( plane ).localizingCursor();
                    final long[] position = new long[ plane.numDimensions() ], nextMin = new long[ plane.numDimensions() ], nextMax = new long[ plane.numDimensions() ];

                    while ( planeCursor.hasNext() )
                    {
                        final long x = planeCursor.next().getLong();
                        if ( x != 0 )
                        {
                            planeCursor.localize( position );
                            for ( int k = 0; k < position.length; ++k )
                            {
                                if ( k != d )
                                {
                                    nextMin[ k ] = Math.max( position[ k ] - 1, 0 );
                                    nextMax[ k ] = Math.min( position[ k ] + 1, labeling.max( k ) );
                                }
                            }
                            nextMin[ d ] = nextMax[ d ] = position[ d ] + 1;

                            final RandomAccessibleInterval< UnsignedLongType > neighborhood = Views.interval( labeling, nextMin, nextMax );
                            final Cursor< UnsignedLongType > neighborhoodCursor = Views.iterable( neighborhood ).cursor();
                            while ( neighborhoodCursor.hasNext() )
                            {
                                final long y = neighborhoodCursor.next().get();
                                if ( y != 0 )
                                    blockTouchingPairs.add( Arrays.asList( x, y ) );
                            }
                        }
                    }
                }
                else
                {
                    throw new IllegalArgumentException( "Unknown or null neighborhood shape type: " + neighborhoodShapeType );
                }
            }
            return blockTouchingPairs;
        } )
                .treeReduce(
                        ( a, b ) -> { a.addAll( b ); return a; },
                        Integer.MAX_VALUE // max possible aggregation depth
        );

        // perform union find to merge all touching objects across blocks
        final TLongLongHashMap parentsMap = new TLongLongHashMap();
        final UnionFind unionFind = new LongHashMapUnionFind( parentsMap, 0, Long::compare );
        for ( final List< Long > touchingPair : touchingPairs )
        {
            unionFind.join(
                    unionFind.findRoot( touchingPair.get( 0 ) ),
                    unionFind.findRoot( touchingPair.get( 1 ) ) );
        }
        Arrays.stream( parentsMap.keys() ).forEach( unionFind::findRoot );

        return parentsMap;
    }

    private static void relabelBlockwiseComponents(
            final JavaSparkContext sparkContext,
            final N5WriterSupplier n5Supplier,
            final String tempDatasetPath,
            final String outputDatasetPath,
            final TLongLongHashMap parentsMap ) throws IOException
    {
        final DatasetAttributes outputDatasetAttributes = n5Supplier.get().getDatasetAttributes( outputDatasetPath );
        final long[] dimensions = outputDatasetAttributes.getDimensions();
        final int[] blockSize = outputDatasetAttributes.getBlockSize();

        final long numBlocks = Intervals.numElements( new CellGrid( dimensions, blockSize ).getGridDimensions() );
        final List< Long > blockIndexes = LongStream.range( 0, numBlocks ).boxed().collect( Collectors.toList() );

        final Broadcast< TLongLongHashMap > parentsMapBroadcast = sparkContext.broadcast( parentsMap );

        sparkContext.parallelize( blockIndexes, Math.min( blockIndexes.size(), MAX_PARTITIONS ) ).foreach( outputBlockIndex ->
        {
            final Interval blockInterval = GridUtils.getCellInterval( new CellGrid( dimensions, blockSize ), outputBlockIndex );
            final N5Writer n5Local = n5Supplier.get();
            final TLongLongHashMap parentsMapLocal = parentsMapBroadcast.getValue();
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
                    outputVal.set( parentsMapLocal.containsKey( inputId ) ? parentsMapLocal.get( inputId ) : inputId );
            }

            N5Utils.saveNonEmptyBlock(
                    outputBlock,
                    n5Local,
                    outputDatasetPath,
                    new UnsignedLongType() );
        } );

        parentsMapBroadcast.destroy();
    }

    public static void main( final String... args ) throws IOException
    {
        final Arguments parsedArgs = new Arguments( args );
        try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
                .setAppName( "N5ConnectedComponentsSpark" )
        ) )
        {
            connectedComponents(
                    sparkContext,
                    () -> new N5FSWriter( parsedArgs.n5Path ),
                    parsedArgs.inputDatasetPath,
                    parsedArgs.outputDatasetPath,
                    parsedArgs.neighborhoodShapeType,
                    Optional.ofNullable( parsedArgs.threshold ),
                    Optional.ofNullable( parsedArgs.blockSize ),
                    Optional.ofNullable( parsedArgs.n5Compression != null ? parsedArgs.n5Compression.get() : null )
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
                usage = "Path to the input dataset within the N5 container (e.g. data/group/s0).")
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

        @Option(name = "-s", aliases = { "--shape" }, required = false,
                usage = "Shape of the neighborhood used to determine if pixels belong together or are located in separate components." +
                        "Can be either diamond (only adjacent pixels are included) or box (includes corner pixels as well).")
        private NeighborhoodShapeType neighborhoodShapeType = NeighborhoodShapeType.Diamond;

        @Option(name = "-t", aliases = { "--threshold" }, required = false,
                usage = "Threshold (min) value to generate binary mask from the input data. By default all positive values are included.")
        private Double threshold;

        private int[] blockSize;

        public Arguments( final String... args )
        {
            final CmdLineParser parser = new CmdLineParser( this );
            try
            {
                parser.parseArgument( args );
                blockSize = blockSizeStr != null ? CmdUtils.parseIntArray( blockSizeStr ) : null;
            }
            catch ( final CmdLineException e )
            {
                System.err.println( e.getMessage() );
                parser.printUsage( System.err );
                System.exit( 1 );
            }
        }
    }
}
