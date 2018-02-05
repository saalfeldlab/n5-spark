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

public class N5ScalePyramidHalfPixelOffsetDownsamplerSpark
{
	public static final String DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY = "downsamplingFactors";
	public static final String OFFSETS_ATTRIBUTE_KEY = "offsets";

	/**
	 * Generates a scale pyramid for a given dataset. Each scale level is downsampled by the specified factors with half-pixel offset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * Reuses the block size of the input dataset. Stores the output datasets in the same group as the input datset.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 *
	 * @return downsampled dataset paths within the same N5 container
	 */
	public static List< String > downsampleScalePyramidWithHalfPixelOffset(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath,
			final int[] downsamplingStepFactors,
			final boolean[] dimensionsWithOffset ) throws IOException
	{
		final String outputGroupPath = ( Paths.get( datasetPath ).getParent() != null ? Paths.get( datasetPath ).getParent().toString() : "" );
		return downsampleScalePyramidWithHalfPixelOffset(
				sparkContext,
				n5Supplier,
				datasetPath,
				outputGroupPath,
				downsamplingStepFactors,
				dimensionsWithOffset
			);
	}

	/**
	 * Generates a scale pyramid for a given dataset. Each scale level is downsampled by the specified factors with half-pixel offset.
	 * Stops generating scale levels once the size of the resulting volume is smaller than the block size in any dimension.
	 * Reuses the block size of the input dataset. Stores the output datasets under the given output group.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to the full-scale dataset
	 *
	 * @return downsampled dataset paths within the same N5 container
	 */
	public static List< String > downsampleScalePyramidWithHalfPixelOffset(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String datasetPath,
			final String outputGroupPath,
			final int[] downsamplingStepFactors,
			final boolean[] dimensionsWithOffset ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		final DatasetAttributes fullScaleAttributes = n5.getDatasetAttributes( datasetPath );
		final long[] dimensions = fullScaleAttributes.getDimensions();
		final int dim = dimensions.length;

		final String intermediateOutputGroupPath = Paths.get( outputGroupPath, "intermediate-downsampling" ).toString();

		// generate power of two scale pyramid
		for ( int scale = 1; ; ++scale )
		{
			final int[] scaleFactors = new int[ dim ];
			for ( int d = 0; d < dim; ++d )
				scaleFactors[ d ] = ( int ) Math.round( Math.pow( downsamplingStepFactors[ d ], scale ) );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				downsampledDimensions[ d ] = dimensions[ d ] / scaleFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
				break;

			final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( intermediateOutputGroupPath, "s" + ( scale - 1 ) ).toString();
			final String outputDatasetPath = Paths.get( intermediateOutputGroupPath, "s" + scale ).toString();

			N5DownsamplerSpark.downsample(
					sparkContext,
					n5Supplier,
					inputDatasetPath,
					outputDatasetPath,
					downsamplingStepFactors
				);

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleFactors );
		}

		final long[] relativeOffset = new long[ dim ];
		for ( int d = 0; d < dim; ++d )
			if ( dimensionsWithOffset[ d ] )
				relativeOffset[ d ] = 1;

		final List< String > downsampledDatasets = new ArrayList<>();

		// generate half-pixel shifted scale pyramid
		for ( int scale = 1; ; ++scale )
		{
			final int[] scaleFactors = new int[ dim ];
			for ( int d = 0; d < dim; ++d )
				scaleFactors[ d ] = ( int ) Math.round( Math.pow( downsamplingStepFactors[ d ], scale ) );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				downsampledDimensions[ d ] = ( dimensions[ d ] + relativeOffset[ d ] ) / scaleFactors[ d ];

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() < 1 )
				break;

			final String inputDatasetPath = scale == 1 ? datasetPath : Paths.get( intermediateOutputGroupPath, "s" + ( scale - 1 ) ).toString();
			if ( !n5.datasetExists( inputDatasetPath ) )
				break;

			final String outputDatasetPath = Paths.get( outputGroupPath, "s" + scale ).toString();

			N5DownsamplerSpark.downsampleWithOffset(
					sparkContext,
					n5Supplier,
					inputDatasetPath,
					outputDatasetPath,
					downsamplingStepFactors,
					relativeOffset
				);

			final long[] offset = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				offset[ d ] = Math.round( Math.pow( downsamplingStepFactors[ d ], scale - 1 ) );

			n5.setAttribute( outputDatasetPath, DOWNSAMPLING_FACTORS_ATTRIBUTE_KEY, scaleFactors );
			n5.setAttribute( outputDatasetPath, OFFSETS_ATTRIBUTE_KEY, offset );

			downsampledDatasets.add( outputDatasetPath );
		}

		N5RemoveSpark.remove( sparkContext, n5Supplier, intermediateOutputGroupPath );
		return downsampledDatasets;
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );

		final boolean[] dimensionsWithOffset = new boolean[ parsedArgs.getOffset().length ];
		for ( int d = 0; d < dimensionsWithOffset.length; ++d )
		{
			if ( parsedArgs.getOffset()[ d ] == 0 )
				dimensionsWithOffset[ d ] = false;
			else if ( parsedArgs.getOffset()[ d ] == 1 )
				dimensionsWithOffset[ d ] = true;
			else
				throw new IllegalArgumentException( "In this version offset can only be 0 or 1" );
		}

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5DownsamplingSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );

			if ( parsedArgs.getOutputGroupPath() != null )
			{
				downsampleScalePyramidWithHalfPixelOffset(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getOutputGroupPath(),
						parsedArgs.getDownsamplingFactors(),
						dimensionsWithOffset
					);
			}
			else
			{
				downsampleScalePyramidWithHalfPixelOffset(
						sparkContext,
						n5Supplier,
						parsedArgs.getInputDatasetPath(),
						parsedArgs.getDownsamplingFactors(),
						dimensionsWithOffset
					);
			}
		}
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

		@Option(name = "-o", aliases = { "--outputGroupPath" }, required = false,
				usage = "Path to a group within the N5 container to store the output datasets (e.g. data/group/scale-pyramid).")
		private String outputGroupPath;

		@Option(name = "-f", aliases = { "--factors" }, required = true,
				usage = "Downsampling factors.")
		private String downsamplingFactors;

		@Option(name = "--offset", required = true,
				usage = "Offset.")
		private String offset;

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
		public String getOutputGroupPath() { return outputGroupPath; }
		public int[] getDownsamplingFactors() { return CmdUtils.parseIntArray( downsamplingFactors ); }
		public long[] getOffset() { return CmdUtils.parseLongArray( offset ); }
	}
}
