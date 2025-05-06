#!/usr/bin/env python3

import os
import sys
import subprocess

sys.dont_write_bytecode = True
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jar_path_util import get_java_cmd_local

subprocess.call(get_java_cmd_local() + ['org.janelia.saalfeldlab.n5.spark.N5MaxIntensityProjectionSpark'] + sys.argv[1:])
