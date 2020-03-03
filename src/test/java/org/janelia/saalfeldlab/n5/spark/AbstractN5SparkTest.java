package org.janelia.saalfeldlab.n5.spark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class AbstractN5SparkTest implements Serializable
{
    protected String basePath;
    protected transient JavaSparkContext sparkContext;

    @Before
    public void setUp()
    {
        sparkContext = new JavaSparkContext( new SparkConf()
                .setMaster( "local[*]" )
                .setAppName( getClass().getName() )
                .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
        );

        basePath = System.getProperty( "user.home" ) + "/.n5-spark-test-" + RandomStringUtils.randomAlphanumeric( 5 );
    }

    @After
    public void tearDown() throws IOException
    {
        if ( sparkContext != null )
            sparkContext.close();

        if ( Files.exists( Paths.get( basePath ) ) )
            Assert.assertTrue( new N5FSWriter( basePath ).remove() );
    }
}
