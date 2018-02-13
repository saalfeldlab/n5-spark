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
git clone --recursive https://github.com/saalfeldlab/n5-spark.git 
```

If you have already cloned the repository, run this after cloning to fetch the submodules:
```bash
git submodule update --init --recursive
```

To use as a standalone tool, compile the package for the desired execution environment:

<details>
<summary><b>Compile for running on Janelia cluster</b></summary>

```bash
python build.py
```
</details>

<details>
<summary><b>Compile for running on local machine</b></summary>

```bash
python build-spark-local.py
```
</details>
<br/>

The scripts for starting the application are located under `startup-scripts/spark-janelia` and `startup-scripts/spark-local`, and their usage is explained below.

If running locally, you can access the Spark job tracker at http://localhost:4040/ to monitor the progress of the tasks.


### N5 downsampling

Generates a single downsampled export:

* <b>N-dimensional downsampling</b>: performs a single downsampling step with given factors.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-downsample.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset> 
  -f <downsampling factors> 
  [--offset]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-downsample.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset> 
  -f <downsampling factors> 
  [--offset]
  ```
  </details>
  
* <b>N-dimensional label downsampling</b>: performs a single downsampling step with given factors. The most frequent value is used instead of averaging. In case of multiple values with the same frequency, the smallest value among them is selected.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-downsample-label.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset>
  -f <downsampling factors> 
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-downsample-label.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset>
  -f <downsampling factors> 
  ```
  </details>

Generates a scale pyramid:

* <b>N-dimensional scale pyramid</b>: generates a scale pyramid with given factors.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  [-o <output group>]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-scale-pyramid.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  [-o <output group>]
  ```
  </details>
  
* <b>N-dimensional scale pyramid with half-pixel offset</b>: generates a scale pyramid with given factors and half-pixel offset applied on every scale level.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid-half-pixel-offset.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  --offset <which dimensions to apply offset to>
  [-o <output group>]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-scale-pyramid-half-pixel-offset.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  --offset <which dimensions to apply offset to>
  [-o <output group>]
  ```
  </details>
  
* <b>3D isotropic scale pyramid</b>: generates a power-of-two scale pyramid adjusting Z downsampling factors to the closest to isotropic with respect to X/Y. Z block sizes are adjusted as well to be the closest to isotropic multiple of the original Z block size.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid-isotropic-3d.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -r <pixel resolution> 
  [-o <output group>]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-scale-pyramid-isotropic-3d.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -r <pixel resolution> 
  [-o <output group>]
  ```
  </details>

If the output group argument is omitted for scale pyramid exporters, the resulting datasets are stored in the same group with the input dataset. The naming scheme for the lower resolution datasets is `s1`, `s2`, `s3` and so on.<br/>
The resulting datasets have the same block size as the given input dataset. Their respective downsampling factors are written into the attributes metadata of the lower resolution datasets.


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
[-d <slice dimension>]
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
[-d <slice dimension>]
```
</details>

The tool converts a given dataset into slice TIFF series and saves them in the specified output folder.<br/>
The following TIFF compression modes are supported: `-c lzw` (default) and `-c none`.<br/>
The slice dimension can be specified as `-d x`, `-d y`, or `-d z` (default) to generate YZ, XZ, or XY slices respectively.


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
