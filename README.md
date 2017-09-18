# n5-spark
A small library for processing N5 datasets in parallel using Apache Spark cluster.

Supported operations:
* downsampling (isotropic/non-isotropic)
* max intensity projection
* conversion to TIFF series
* parallel remove

### Usage

To use the library in your Spark-based project, add a maven dependency and make sure that your application is set to be compiled as a fat jar.
