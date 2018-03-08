# n5-spark
A small library for processing N5 datasets in parallel using Apache Spark cluster.

Supported operations:
* resaving using different blocksize / datatype / compression
* downsampling (isotropic / non-isotropic)
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


### N5 converter

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-convert.py 
<number of cluster nodes> 
-ni <path to input n5 root> 
-i <input dataset>
[-no <path to output n5 root if not the same as input n5>]
-o <output dataset>
[-b <output block size>]
[-c <output compression scheme>]
[-t <output data type>]
[-min <min value of input data range>]
[-max <max value of input data range>]
[--force to overwrite output dataset if already exists]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-convert.py 
-ni <path to input n5 root> 
-i <input dataset>
[-no <path to output n5 root if not the same as input n5>]
-o <output dataset>
[-b <output block size>]
[-c <output compression scheme>]
[-t <output data type>]
[-min <min value of input data range>]
[-max <max value of input data range>]
[--force to overwrite output dataset if already exists]
```
</details>

<br/>

Resaves an N5 dataset possibly changing all or some of the following dataset attributes:
* *block size*: if omitted, the block size of the input dataset is used.
* *compression scheme*: if omitted, the compression scheme of the input dataset is used.
* *data type*: if omitted, the data type of the input dataset is used.<br/>
If specified and is different from the input dataset type, the values are mapped from the input value range to the output value range.<br/>
The optional `-min` and `-max` arguments specify the input data value range. If omitted, the input value range is derived from the input data type for integer types, or set to `[0,1]` for real types by default.<br/>
The output value range is derived from the output data type for integer types, or set to `[0,1]` for real types.


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
  [-b <block size>]
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
  [-b <block size>]
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
  [-b <block size>]
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
  [-b <block size>]
  ```
  </details>

* <b>N-dimensional offset downsampling</b>: performs a single downsampling step with given factors and offset.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-downsample-offset.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset> 
  -f <downsampling factors> 
  -s <offset>
  [-b <block size>]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-downsample-offset.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -o <output dataset> 
  -f <downsampling factors> 
  -s <offset>
  [-b <block size>]
  ```
  </details>

Generates a scale pyramid:

* <b>N-dimensional scale pyramid</b>: generates a scale pyramid with given factors. The downsampling factors parameter specifies relative scaling between any two consecutive scale levels in the output scale pyramid.
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
  
* <b>N-dimensional offset scale pyramid</b>: generates a scale pyramid with given factors and half-pixel offset applied at every scale level. The downsampling factors parameter specifies relative scaling between any two consecutive scale levels in the output scale pyramid.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid-offset.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  -s <which dimensions to apply offset to>
  [-o <output group>]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-scale-pyramid-offset.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -f <downsampling factors> 
  -s <which dimensions to apply offset to>
  [-o <output group>]
  ```
  </details>
  
* <b>3D non-isotropic scale pyramid</b>: generates a scale pyramid with power of two downsampling in X/Y and adjustment of Z downsampling factors and Z block sizes to the closest to isotropic with respect to X/Y.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid-nonisotropic-3d.py 
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
  spark-local/n5-scale-pyramid-nonisotropic-3d.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -r <pixel resolution> 
  [-o <output group>]
  ```
  </details>

If the output group argument is omitted for scale pyramid exporters, the resulting datasets will be stored in the same group with the input dataset. The naming scheme for the lower resolution datasets is `s1`, `s2`, `s3` and so on.<br/>
If the block size argument is omitted, the resulting dataset will have the same block size as the input dataset. Downsampling factors are written into the attributes metadata of the lower resolution datasets.


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
