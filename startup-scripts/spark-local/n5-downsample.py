#!/usr/bin/env python

import os
import sys
import subprocess

curr_script_dir = os.path.dirname(os.path.realpath(__file__))
base_folder = os.path.dirname(os.path.dirname(curr_script_dir))
bin_file = os.path.join('target', 'n5-spark-1.0.1-SNAPSHOT.jar')
bin_path = os.path.join(base_folder, bin_file)

subprocess.call(['java', '-Dspark.master=local[*]', '-cp', bin_path, 'org.janelia.saalfeldlab.n5.spark.N5DownsamplingSpark'] + sys.argv[1:])