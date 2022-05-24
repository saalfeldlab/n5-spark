#!/usr/bin/env python3

import os
import sys
import subprocess

sys.dont_write_bytecode = True
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jar_path_util import get_local_jar_path
bin_path = get_local_jar_path()

subprocess.call(['java', '-Dspark.master=local[*]', '-cp', bin_path, 'org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5ScalePyramidSpark'] + sys.argv[1:])
