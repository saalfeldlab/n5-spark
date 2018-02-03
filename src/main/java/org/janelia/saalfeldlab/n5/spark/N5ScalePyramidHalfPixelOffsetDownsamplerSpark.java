package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class N5ScalePyramidHalfPixelOffsetDownsamplerSpark
{
	private static final int MAX_PARTITIONS = 15000;
	private static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";
	private static final String OFFSET_ATTRIBUTE_KEY = "offset";

	/**
	 * Generates a scale pyramid for a given dataset. Each scale level is downsampled by the factor of 2 in all dimensions using half-pixel offset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * Reuses the block size of the given dataset.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 *
	 * @return downsampling factors for all scales including the input (full scale)
	 */
	public static Pair< int[][], long[][] > downsampleScalePyramidWithHalfPixelOffset(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = fullScaleAttributes.getDimensions();
		final int dim = dimensions.length;

		final int[] relativeDownsamplingFactors = new int[ dim ];
		Arrays.fill( relativeDownsamplingFactors, 2 );

		final String outputPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		final String intermediateOutputPath = Paths.get( outputPath, "intermediate-downsampling" ).toString();

		// generate power of two scale pyramid
		for ( int scale = 1; ; ++scale )
		{
			final int[] scaleFactors = new int[ dim ];
			for ( int d = 0; d < dim; ++d )
				scaleFactors[ d ] = ( int ) Math.round( Math.pow( relativeDownsamplingFactors[ d ], scale ) );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				downsampledDimensions[ d ] = dimensions[ d ] / scaleFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
				break;

			final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( intermediateOutputPath, "s" + ( scale - 1 ) ).toString();
			final String outputDatasetPath = Paths.get( intermediateOutputPath, "s" + scale ).toString();

			N5DownsamplerSpark.downsample(
					sparkContext,
					n5Supplier,
					inputDatasetPath,
					outputDatasetPath,
					relativeDownsamplingFactors
				);

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleFactors );
		}

		final long[] relativeOffset = new long[ dim ];
		Arrays.fill( relativeOffset, 1 );

		final List< int[] > scales = new ArrayList<>();
		{
			final int[] fullScaleDownsamplingFactors = new int[ dim ];
			Arrays.fill( fullScaleDownsamplingFactors, 1 );
			scales.add( fullScaleDownsamplingFactors );
		}

		final List< long[] > offsets = new ArrayList<>();
		offsets.add( new long[ dim ] );

		// generate half-pixel shifted scale pyramid
		for ( int scale = 1; ; ++scale )
		{
			final int[] scaleFactors = new int[ dim ];
			for ( int d = 0; d < dim; ++d )
				scaleFactors[ d ] = ( int ) Math.round( Math.pow( relativeDownsamplingFactors[ d ], scale ) );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				downsampledDimensions[ d ] = ( dimensions[ d ] + relativeOffset[ d ] ) / scaleFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
				break;

			final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( intermediateOutputPath, "s" + ( scale - 1 ) ).toString();
			if ( !n5.datasetExists( inputDatasetPath ) )
				break;

			final String outputDatasetPath = Paths.get( outputPath, "s" + scale ).toString();

			N5DownsamplerSpark.downsampleWithOffset(
					sparkContext,
					n5Supplier,
					inputDatasetPath,
					outputDatasetPath,
					relativeDownsamplingFactors,
					relativeOffset
				);

			final long[] offset = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				offset[ d ] = Math.round( Math.pow( relativeDownsamplingFactors[ d ], scale - 1 ) );

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleFactors );
			n5.setAttribute( outputDatasetPath, OFFSET_ATTRIBUTE_KEY, offset );

			scales.add( scaleFactors );
			offsets.add( offset );
		}

		N5RemoveSpark.remove( sparkContext, n5Supplier, intermediateOutputPath );

		return new ValuePair<>( scales.toArray( new int[ 0 ][] ), offsets.toArray( new long[ 0 ][] ) );
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );

		final Pair< int[][], long[][] > scalesAndOffsets;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5DownsamplingSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
			scalesAndOffsets = downsampleScalePyramidWithHalfPixelOffset(
					sparkContext,
					n5Supplier,
					parsedArgs.getInputDatasetPath()
				);
		}

		System.out.println();
		System.out.println( "Scale levels:" );
		for ( int i = 0; i < scalesAndOffsets.getA().length; ++i )
			System.out.println( "  " + i + ": " + Arrays.toString( scalesAndOffsets.getA()[ i ] ) );
		System.out.println( "Offsets:" );
		for ( int i = 0; i < scalesAndOffsets.getB().length; ++i )
			System.out.println( "  " + i + ": " + Arrays.toString( scalesAndOffsets.getB()[ i ] ) );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -1467734459169624759L;

		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to an input dataset within the N5 container (e.g. data/group/s0).")
		private String inputDatasetPath;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
				System.exit( 1 );
			}
		}

		public String getN5Path() { return n5Path; }
		public String getInputDatasetPath() { return inputDatasetPath; }
	}
}
