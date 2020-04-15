# N5 Spark [![Build Status](https://travis-ci.org/saalfeldlab/n5-spark.svg?branch=master)](https://travis-ci.org/saalfeldlab/n5-spark)
A collection of utilities for [N5](https://github.com/saalfeldlab/n5) datasets that can run on an Apache Spark cluster.

Supported operations:
* thresholding and labeling of connected components
* resaving using different blocksize / datatype / compression
* downsampling (isotropic / non-isotropic)
* max intensity projection
* conversion between N5 and slice TIFF series
* parallel remove

N5 Spark can run on **Amazon Web Services** and **Google Cloud**, more information is available in the [wiki](https://github.com/saalfeldlab/n5-spark/wiki).

## Usage

Clone the repository with submodules:

```bash
git clone --recursive https://github.com/saalfeldlab/n5-spark.git 
```

If you have already cloned the repository, run this after cloning to fetch the submodules:
```bash
git submodule update --init --recursive
```

You can use the library in your Spark-based project or as a standalone tool. To use in your project, add a maven dependency and make sure that your application is set to be compiled as a shaded jar that contains all dependencies.

Alternatively, to use as a standalone tool, compile the package for the desired execution environment:

<details>
<summary><b>Compile for running on Janelia cluster</b></summary>

```bash
python build.py
```

While OpenJDK and Maven's surefire plugin have this joint [bug](https://stackoverflow.com/questions/53010200/maven-surefire-could-not-find-forkedbooter-class), you will have to build with
```bash
_JAVA_OPTIONS=-Djdk.net.URLClassPath.disableClassPathURLCheck=true ./build.py
```

</details>

<details>
<summary><b>Compile for running on local machine</b></summary>

```bash
python build-spark-local.py
```

While OpenJDK and Maven's surefire plugin have this joint [bug](https://stackoverflow.com/questions/53010200/maven-surefire-could-not-find-forkedbooter-class), you will have to build with
```bash
_JAVA_OPTIONS=-Djdk.net.URLClassPath.disableClassPathURLCheck=true ./build-spark-local.py
```
</details>
<br/>

The scripts for starting the application are located under `startup-scripts/spark-janelia` and `startup-scripts/spark-local`, and their usage is explained below.

If running locally, you can access the Spark job tracker at http://localhost:4040/ to monitor the progress of the tasks.



### N5 connected components

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-connected-components.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset>
-o <output dataset>
[-t <min threshold value for input data>]
[-s <neighborhood shape, can be 'diamond' or 'box'>]
[-m <min size of accepted connected components in pixels>]
[-b <output block size>]
[-c <output compression scheme>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-connected-components.py 
-n <path to n5 root> 
-i <input dataset>
-o <output dataset>
[-t <min threshold value for input data>]
[-s <neighborhood shape, can be 'diamond' or 'box'>]
[-m <min size of accepted connected components in pixels>]
[-b <output block size>]
[-c <output compression scheme>]
```
</details>

Finds and labels all connected components in a binary mask extracted from the input N5 dataset and saves the relabeled dataset as an `uint64` output dataset.

Optional parameters:
* **Threshold**: min value in the input data to be included in the binary mask. The condition is `input >= threshold`. If omitted, all input values higher than 0 are included in the binary mask (`input > 0`).
* **Neighborhood shape**: specifies how pixels are grouped into connected components. There are two options:
  * Diamond (default): only direct neighbors are considered (4-neighborhood in 2D, 6-neighborhood in 3D).
  * Box (`-s box`): diagonal pixels are considered as well (8-neighborhood in 2D, 26-neighborhood in 3D). It can be used to decrease the number of trivial components.
* **Min size**: minimum size of a connected component in pixels. If specified, components that contain fewer number of pixels are discarded from the resulting set. By default all components are kept.
* *block size*: comma-separated list. If omitted, the block size of the input dataset is used.
* *compression scheme*: if omitted, the compression scheme of the input dataset is used.

When the processing is completed, some statistics about the extracted connected components will be stored in the file `stats.txt` in the directory with the output dataset (works only for filesystem-based N5).


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

Resaves an N5 dataset possibly changing all or some of the following dataset attributes:
* *block size*: if omitted, the block size of the input dataset is used.
* *compression scheme*: if omitted, the compression scheme of the input dataset is used.
* *data type*: if omitted, the data type of the input dataset is used.<br/>
If specified and is different from the input dataset type, the values are mapped from the input value range to the output value range.<br/>
The optional `-min` and `-max` arguments specify the input data value range. If omitted, the input value range is derived from the input data type for integer types, or set to `[0,1]` for real types by default.<br/>
The output value range is derived from the output data type for integer types, or set to `[0,1]` for real types.


### N5 downsampling

Generates a single downsampled export:

* <b>N-dimensional downsampling</b>: performs a single downsampling step with given factors. The downsampling factors parameter is formatted as a comma-separated list, for example, `2,2,2`.
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
  
* <b>N-dimensional label downsampling</b>: performs a single downsampling step with given factors. The downsampling factors parameter is formatted as a comma-separated list, for example, `2,2,2`.<br/>
The most frequent value is used instead of averaging. In case of multiple values with the same frequency, the smallest value among them is selected.
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

* <b>N-dimensional offset downsampling</b>: performs a single downsampling step with given factors and offset. The downsampling factors and offset parameters are formatted as comma-separated lists, for example, `2,2,2`.
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

* <b>N-dimensional scale pyramid</b>: generates a scale pyramid with given factors. The downsampling factors parameter specifies relative scaling between any two consecutive scale levels in the output scale pyramid, and is formatted as a comma-separated list, for example, `2,2,2`.
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
  
* <b>N-dimensional offset scale pyramid</b>: generates a scale pyramid with given factors and half-pixel offset applied at every scale level. The downsampling factors parameter specifies relative scaling between any two consecutive scale levels in the output scale pyramid, and is formatted as a comma-separated list, for example, `2,2,2`.
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
  
* <b>Non-isotropic scale pyramid</b>: generates a scale pyramid of a dataset with different resolution in X/Y and Z. Depending on whether the resolution is better in X/Y than in Z or vice versa, the downsampling factors are adjusted to make the scale levels as close to isotropic as possible. The pixel resolution parameter is given in um (microns) formatted as a comma-separated list, for example, `0.097,0.097,0.18`.<br/>
If the optional argument `-p` is provided, all downsampling factors are forced to be powers of two. This mode is faster as it does not require any intermediate downsampling steps.<br/>
Only the first 3 dimensions of the input data are downsampled. If the input data is of higher dimensionality than 3, the rest of the dimensions are written out as is.
  <details>
  <summary><b>Run on Janelia cluster</b></summary>
  
  ```bash
  spark-janelia/n5-scale-pyramid-nonisotropic.py 
  <number of cluster nodes> 
  -n <path to n5 root> 
  -i <input dataset> 
  -r <pixel resolution> 
  [-o <output group>]
  [-p]
  ```
  </details>  
  <details> 
  <summary><b>Run on local machine</b></summary>
  
  ```bash
  spark-local/n5-scale-pyramid-nonisotropic.py 
  -n <path to n5 root> 
  -i <input dataset> 
  -r <pixel resolution> 
  [-o <output group>]
  [-p]
  ```
  </details>

If the output group argument is omitted for scale pyramid exporters, the resulting datasets will be stored in the same group with the input dataset. The naming scheme for the lower resolution datasets is `s1`, `s2`, `s3` and so on.<br/>
If the block size argument is omitted, the resulting dataset will have the same block size as the input dataset. Downsampling factors are written into the attributes metadata of the lower resolution datasets.


### Slice TIFF series to N5 converter

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/slice-tiff-to-n5.py
<number of cluster nodes>
-i <input directory>
-n <output n5 root>
-o <output dataset>
-b <output block size>
[-c <n5 compression>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/slice-tiff-to-n5.py
-i <input directory>
-n <output n5 root>
-o <output dataset>
-b <output block size>
[-c <n5 compression>]
```
</details>

The tool lists all slice TIFF images contained in the input directory and converts them into a 3D N5 dataset.<br/>
The slice images are automatically sorted by their filenames in natural order, such that `1.tif` and `2.tif` are placed before `10.tif`.<br/>
The block size can be specified as three comma-separated values, or as a single value as a shortcut for cube-shaped blocks.<br/>
The input images are assumed to be XY slices.


### N5 to slice TIFF series converter

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-to-slice-tiff.py
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-d <slice dimension>]
[-c <tiff compression>]
[-f <filename format>]
[--fill <fill value>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-to-slice-tiff.py
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-d <slice dimension>]
[-c <tiff compression>]
[-f <filename format>]
[--fill <fill value>]
```
</details>

The tool converts a given dataset into slice TIFF series and saves them in the specified output folder.<br/>
The slice dimension can be specified as `-d x`, `-d y`, or `-d z` (default) to generate YZ, XZ, or XY slices respectively.

The filename format is expected to contain a single placeholder for an integer representing the index of the slice, such as:
* `-f slice%dz.tif` to produce filenames such as *slice0z.tif*, *slice1z.tif*, *slice2z.tif*, etc.
* `-f slice%03dz.tif` to produce filenames such as *slice000z.tif*, *slice001z.tif*, *slice002z.tif*, etc.

The default pattern is `%d.tif` so the resulting filenames by default are simply *0.tif*, *1.tif*, *2.tif*, etc.

If the input dataset is sparse and some of the N5 blocks do not exist, this empty space will be filled in the TIFF images with 0 by default. The `--fill` parameter allows to change this fill value.

Output TIFF images are written as uncompressed by default. LZW compression can be enabled by supplying `-c lzw`.<br/>
**WARNING:** LZW compressor can be very slow. It is not recommended for general use unless saving disk space is crucial.


### N5 max intensity projection

<details>
<summary><b>Run on Janelia cluster</b></summary>

```bash
spark-janelia/n5-mips.py 
<number of cluster nodes> 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-m <mip step>]
[-c <tiff compression>]
```
</details>

<details>
<summary><b>Run on local machine</b></summary>

```bash
spark-local/n5-mips.py 
-n <path to n5 root> 
-i <input dataset> 
-o <output path> 
[-m <mip step>]
[-c <tiff compression>]
```
</details>

The tool generates max intensity projections in X/Y/Z directions and saves them as TIFF images in the specified output folder.<br/>
By default the entire volume is used to create a single MIP in X/Y/Z. You can specify MIP step as a number of cells included in a single MIP (e.g. `-m 5,5,3`).<br/>

Output TIFF images are written as uncompressed by default. LZW compression can be enabled by supplying `-c lzw`.<br/>
**WARNING:** LZW compressor can be very slow. It is not recommended for general use unless saving disk space is crucial.


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
