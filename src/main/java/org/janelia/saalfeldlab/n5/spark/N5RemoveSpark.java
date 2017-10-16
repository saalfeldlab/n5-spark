package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;

public class N5RemoveSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Removes an N5 group or dataset parallelizing over inner groups.
	 *
	 * @param sparkContext
	 * 			Spark context
	 * @param n5
	 * 			N5 container
	 * @param datasetPath
	 * 			Path to a group or dataset to be removed
	 */
	public static boolean remove(
			final JavaSparkContext sparkContext,
			final N5Writer n5,
			final String pathName ) throws IOException
	{
		if ( n5.exists( pathName ) )
		{
			final List< String > leaves = new ArrayList<>();
			final List< String > nodesQueue = new ArrayList<>();
			nodesQueue.add( pathName );

			final Broadcast< N5Writer > n5Broadcast = sparkContext.broadcast( n5 );

			// iteratively find all leaves
			while ( !nodesQueue.isEmpty() )
			{
				final Map< String, String[] > nodeToChildren = sparkContext.parallelize( nodesQueue, Math.min( nodesQueue.size(), MAX_PARTITIONS ) ).mapToPair( node -> new Tuple2<>( node, n5Broadcast.value().list( node ) ) ).collectAsMap();
				nodesQueue.clear();
				for ( final Entry< String, String[] > entry : nodeToChildren.entrySet() )
				{
					if ( entry.getValue().length == 0 )
					{
						leaves.add( entry.getKey() );
					}
					else
					{
						for ( final String child : entry.getValue() )
							nodesQueue.add( Paths.get( entry.getKey(), child ).toString() );
					}
				}
			}

			// delete inner files
			sparkContext.parallelize( leaves, Math.min( leaves.size(), MAX_PARTITIONS ) ).foreach( leaf -> n5Broadcast.value().remove( leaf ) );

			n5Broadcast.destroy();
		}

		// cleanup the directory tree
		return n5.remove( pathName );
	}


	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5RemoveSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final N5Writer n5 = N5.openFSWriter(parsedArgs.getN5Path());
			remove( sparkContext, n5, parsedArgs.getInputPath() );
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments
	{
		@Option(name = "-n", aliases = { "--n5Path" }, required = true,
				usage = "Path to an N5 container.")
		private String n5Path;

		@Option(name = "-i", aliases = { "--inputDatasetPath" }, required = true,
				usage = "Path to a group or dataset within the N5 container to be removed (e.g. data/group).")
		private String inputPath;

		private boolean parsedSuccessfully = false;

		public Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
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
		public String getInputPath() { return inputPath; }
	}
}
