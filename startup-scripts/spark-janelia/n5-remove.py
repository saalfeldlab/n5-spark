#!/usr/bin/env python3

import os
import sys
import subprocess

sys.dont_write_bytecode = True
curr_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(curr_script_dir))
from jar_path_util import get_provided_jar_path
bin_path = get_provided_jar_path()

flintstone_relpath = os.path.join('flintstone', 'flintstone-lsd.sh')
flintstone_path = os.path.join(curr_script_dir, flintstone_relpath)

os.environ['N_DRIVER_THREADS'] = '2'
os.environ['MEMORY_PER_NODE'] = '115'

nodes = int(sys.argv[1])

subprocess.call([flintstone_path, str(nodes), bin_path, 'org.janelia.saalfeldlab.n5.spark.N5RemoveSpark'] + sys.argv[2:])
