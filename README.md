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


### N5 Downsampling

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-downsample.py <number of cluster nodes> -n <path to n5 root> -i <input dataset> [-r <pixel resolution>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-downsample.py -n <path to n5 root> -i <input dataset> [-r <pixel resolution>]
```
</details>

The tool generates lower resolution datasets in the same group with the input dataset until the resulting volume fits into a single block. By default the downsampling factors are powers of two (`[2,2,2],[4,4,4],[8,8,8],...`). If the optional pixel resolution parameter is passed (e.g. `-r 0.097,0.097,0.18`), the downsampling factors in Z are adjusted with respect to it to make lower resolutions as close to isotropic as possible.<br/>
The block size of the input dataset is reused, or adjusted with respect to the pixel resolution is the optional parameter is supplied.<br/>
The used downsampling factors are written into the attributes metadata of the lower resolution datasets.


-------------------------------------------------------------

You can alternatively use the library in your Spark-based project. Add a maven dependency and make sure that your application is set to be compiled as a fat jar.
