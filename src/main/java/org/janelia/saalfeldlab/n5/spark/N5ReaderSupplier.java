package org.janelia.saalfeldlab.n5.spark;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.N5Reader;

@FunctionalInterface
public interface N5ReaderSupplier extends Serializable
{
	public N5Reader get() throws IOException;
}
