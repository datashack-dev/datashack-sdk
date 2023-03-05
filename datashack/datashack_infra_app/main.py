#!/usr/bin/env python

import os, sys
sys.path.append(os.path.dirname(__file__))
# sys.path.append(os.path.dirname(__file__) + '/../datashack_infra_app')

import json
import os
import subprocess
import sys
from typing import Dict, List
from cdktf import App
from additional_plugins.datashack_glue import GlueTableConf
from additional_plugins import DatashackStack, DatashackStackConf
import json 
import re
import tempfile
import shutil


def create_app_with_resource(env_id: str, resources):
    app = App()
    

    stack_conf = DatashackStackConf(
        env=env_id,
        resources=resources)

    DatashackStack(app, stack_conf)
    app.synth()


def _move_to_this_dir():
    # move to this folder as we need to be in cdktf context now
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)


if __name__ == '__main__':
    env_id = sys.argv[1]
    resources_output_file = sys.argv[2]
    
    with open(resources_output_file, 'r') as fp:
        resources = json.load(fp)['resources']
    
    create_app_with_resource(env_id, resources)
