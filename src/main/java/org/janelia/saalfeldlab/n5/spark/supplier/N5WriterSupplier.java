package org.janelia.saalfeldlab.n5.spark.supplier;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.N5Writer;

@FunctionalInterface
public interface N5WriterSupplier extends N5ReaderSupplier, Serializable
{
	@Override
	public N5Writer get() throws IOException;
}
