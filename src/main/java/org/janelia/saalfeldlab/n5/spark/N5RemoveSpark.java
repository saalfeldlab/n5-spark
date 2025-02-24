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
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.imglib2.algorithm.util.Singleton;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import scala.Tuple2;

public class N5RemoveSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Removes an N5 container parallelizing over inner groups.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 */
	public static boolean remove(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier ) throws IOException
	{
		return remove( sparkContext, n5Supplier, null );
	}

	/**
	 * Removes an N5 group or dataset parallelizing over inner groups.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param datasetPath
	 * 			Path to a group or dataset to be removed
	 */
	public static boolean remove(
			final JavaSparkContext sparkContext,
			final N5WriterSupplier n5Supplier,
			final String pathName ) throws IOException
	{
		final N5Writer n5 = n5Supplier.get();
		if ( pathName == null || n5.exists( pathName ) )
		{
			final List< String > leaves = new ArrayList<>();
			final List< String > nodesQueue = new ArrayList<>();
			nodesQueue.add( pathName != null ? pathName : "" );

			// iteratively find all leaves
			while ( !nodesQueue.isEmpty() )
			{
				final Map< String, String[] > nodeToChildren = sparkContext
						.parallelize( nodesQueue, Math.min( nodesQueue.size(), MAX_PARTITIONS ) )
						.mapToPair( node -> {
							final N5Writer n5Local = n5Supplier.get();
							final String writerCacheKey = new URIBuilder(n5Local.getURI())
									.setParameters(
											new BasicNameValuePair("type", "writer"),
											new BasicNameValuePair("call", "n5-remove-spark")
									).toString();
							final N5Writer n5Writer = Singleton.get(writerCacheKey, () -> n5Local);
							return new Tuple2<>(node, n5Writer.list(node));
						})
						.collectAsMap();

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
			sparkContext
					.parallelize( leaves, Math.min( leaves.size(), MAX_PARTITIONS ) )
					.foreach( leaf -> {
						final N5Writer n5Local = n5Supplier.get();
						final String writerCacheKey = new URIBuilder(n5Local.getURI())
								.setParameters(
										new BasicNameValuePair("type", "writer"),
										new BasicNameValuePair("call", "n5-remove-spark")
								).toString();
						final N5Writer n5Writer = Singleton.get(writerCacheKey, () -> n5Local);
						n5Writer.remove(leaf);
					});

			Singleton.clear();
		}

		// cleanup the directory tree
		return pathName != null ? n5.remove( pathName ) : n5.remove();
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
			final N5WriterSupplier n5Supplier = () -> new N5FSWriter( parsedArgs.getN5Path() );
			remove( sparkContext, n5Supplier, parsedArgs.getInputPath() );
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static class Arguments implements Serializable
	{
		private static final long serialVersionUID = -5780005567896943578L;

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
