package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.N5Writer;

@FunctionalInterface
public interface N5WriterSupplier extends Serializable
{
	public N5Writer get() throws IOException;
}
