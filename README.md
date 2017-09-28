# n5-spark
A small library for processing N5 datasets in parallel using Apache Spark cluster.

Supported operations:
* downsampling (isotropic/non-isotropic)
* max intensity projection
* conversion to TIFF series
* parallel remove

## Usage

Clone the repository with submodules:

```bash
git clone --recursive https://github.com/saalfeldlab/stitching-spark.git 
```

If you have already cloned the repository, run this after cloning to fetch the submodules:
```bash
git submodule update --init --recursive
```

To use as a standalone tool, compile the package for the desired execution environment:

<details>
<summary><b>Compile for running on Janelia cluster</b></summary>

```bash
mvn clean package
```
</details>

<details>
<summary><b>Compile for running on local machine</b></summary>

```bash
mvn clean package -Pspark-local
```
</details>
<br/>

The scripts for starting the application are located under `startup-scripts/spark-janelia` and `startup-scripts/spark-local`, and their usage is explained below.

If running locally, you can access the Spark job tracker at http://localhost:4040/ to monitor the progress of the tasks.

-------------------------------------------------------------

You can alternatively use the library in your Spark-based project. Add a maven dependency and make sure that your application is set to be compiled as a fat jar.
