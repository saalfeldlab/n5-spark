package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;

import scala.Tuple2;

public class N5RemoveSpark
{
	/**
	 * Removes an N5 group or dataset parallelizing over inner groups.
	 *
	 * @param sparkContext Spark context
	 * @param basePath Path to the N5 root
	 * @param datasetPath Path to a group or dataset to be removed
	 */
	public static boolean remove( final JavaSparkContext sparkContext, final String basePath, final String pathName ) throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		if ( n5.exists( pathName ) )
		{
			final List< String > leaves = new ArrayList<>();
			final List< String > nodesQueue = new ArrayList<>();
			nodesQueue.add( pathName );

			// iteratively find all leaves
			while ( !nodesQueue.isEmpty() )
			{
				final Map< String, String[] > nodeToChildren = sparkContext.parallelize( nodesQueue, nodesQueue.size() ).mapToPair( node -> new Tuple2<>( node, N5.openFSReader( basePath ).list( node ) ) ).collectAsMap();
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
			sparkContext.parallelize( leaves, leaves.size() ).foreach( leaf -> N5.openFSWriter( basePath ).remove( leaf ) );
		}

		// cleanup the directory tree
		return n5.remove( pathName );
	}
}
