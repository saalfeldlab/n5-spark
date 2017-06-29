package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;

public class N5RemoveSpark
{
	public static void remove( final JavaSparkContext sparkContext, final String basePath, final String pathName ) throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );
		final List< String > leaves = new ArrayList<>();
		final Queue< String > nodesQueue = new LinkedList<>();
		nodesQueue.add( pathName );
		while ( !nodesQueue.isEmpty() )
		{
			final String node = nodesQueue.remove();
			final String[] children = n5.list( node );
			if ( children.length == 0 )
				leaves.add( node );
			else
				for ( final String child : children )
					nodesQueue.add( Paths.get( node, child ).toString() );
		}

		sparkContext.parallelize( leaves ).foreach( leaf -> N5.openFSWriter( basePath ).remove( leaf ) );

		n5.remove( pathName );
	}
}
