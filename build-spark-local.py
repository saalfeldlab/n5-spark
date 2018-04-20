#!/usr/bin/env python

import os
import sys
sys.dont_write_bytecode = True
from build import run_build

if __name__ == '__main__':
	base_folder = os.path.dirname(os.path.abspath(__file__))
	build_args = ['-P', 'fatjar,spark-local']
	run_build(base_folder, build_args)
	
	curr_script_dir = os.path.dirname(os.path.abspath(__file__))
	sys.path.append(os.path.join(curr_script_dir, 'startup-scripts'))
	from jar_path_util import get_jar_path, get_local_jar_path
	os.rename(get_jar_path(), get_local_jar_path())
