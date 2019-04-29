package org.janelia.saalfeldlab.n5.spark.util;

import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;

public enum N5Compression
{
	RAW( new RawCompression() ),
	GZIP( new GzipCompression() ),
	BZIP2( new Bzip2Compression() ),
	LZ4( new Lz4Compression() ),
	XZ( new XzCompression() );

	private final Compression compression;

	private N5Compression( final Compression compression )
	{
		this.compression = compression;
	}

	public Compression get()
	{
		return compression;
	}
}
