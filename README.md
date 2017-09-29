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


### N5 downsampling

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-downsample.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset> 
[-r <pixel resolution>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-downsample.py 
-n <path to n5 root> 
-i <input dataset> 
[-r <pixel resolution>]
```
</details>

The tool generates lower resolution datasets in the same group with the input dataset until the resulting volume fits into a single block. The namin scheme for the lower resolution datasets is `s1`, `s2`, `s3` and so on.<br/>
By default the downsampling factors are powers of two (`[2,2,2],[4,4,4],[8,8,8],...`). If the optional pixel resolution parameter is passed (e.g. `-r 0.097,0.097,0.18`), the downsampling factors in Z are adjusted with respect to it to make lower resolutions as close to isotropic as possible.<br/>
The block size of the input dataset is reused, or adjusted with respect to the pixel resolution if the optional parameter is supplied. The used downsampling factors are written into the attributes metadata of the lower resolution datasets.


### N5 to slice TIFF series converter

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-slice-tiff.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-c <tiff compression>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-slice-tiff.py 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-c <tiff compression>]
```
</details>

The tool converts a given dataset into slice TIFF series and saves them in the specified output folder.<br/>
The following TIFF compression modes are supported: `-c lzw` and `-c none`.


### N5 max intensity projection

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-mips.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-c <tiff compression>]
[-m <mip step>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-mips.py 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-c <tiff compression>]
[-m <mip step>]
```
</details>

The tool generates max intensity projections in X/Y/Z directions and saves them as TIFF images in the specified output folder.<br/>
By default the entire volume is used to create a single MIP in X/Y/Z. You can specify MIP step as a number of cells included in a single MIP (e.g. `-m 5,5,3`).<br/>
The following TIFF compression modes are supported: `-c lzw` and `-c none`.


### N5 remove

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-remove.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset or group>
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-remove.py 
-n <path to n5 root> 
-i <input dataset or group>
```
</details>

The tool removes a group or dataset parallelizing over inner groups. This is typically much faster than deleting the group on a single machine, in particular when removing groups with many nested groups and/or n5 blocks.


-------------------------------------------------------------

You can alternatively use the library in your Spark-based project. Add a maven dependency and make sure that your application is set to be compiled as a fat jar.
