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
import json 
import re
import tempfile
import shutil




def clean_stdout(stdout):
    
    return stdout.replace('\u001b[2K', '').replace('\u001b[1A', '').replace('\u001b[G', '').replace('Synthesizing\n', '').replace('\n ', '.')

def apply_imported_packages(account_id: str, env_id: str, resources: List, on_stdout_update=None, output_dir=None, plan=True):
    # remove me
    # return {}

    _move_to_this_dir()
    
    cdktf_input = json.dumps({
            'env_id': env_id,
            'resources': resources
        }).replace('\'', '\\\'')
    py_interp = sys.executable
    app_command = f'{py_interp} main.py \'{cdktf_input}\''
    

    output_dir = output_dir or '.'
    output_file_name = os.path.join(output_dir, 'outputs.json')

    with open("/tmp/test.log", "wb") as f:
        process = subprocess.Popen(["cdktf", "plan" if plan else "deploy", env_id,
                                    "--app", app_command,
                                    '--outputs-file', output_file_name,
                                    '--output', output_dir or '.',
                                    "--outputs-file-include-sensitive-output"], stdout=subprocess.PIPE)
        
        acc_stdout = ''
        for ii, c in enumerate(iter(lambda: process.stdout.read(1), b'')):
            stdout_c = c.decode("utf-8", "ignore")
            sys.stdout.write(stdout_c)
            f.write(c)
    
    if process.poll():
        raise Exception

    if not plan:
        # load outputs and write them to redis
        with open(output_file_name) as outputs_file:
            output_json_data = json.load(outputs_file)
        
        return output_json_data[env_id]


def delete_imported_packages(account_id: str, env_id: str, resources: List, output_dir=None):
    # remove me    
    _move_to_this_dir()

    cdktf_input = json.dumps({
            'env_id': env_id,
            'resources': resources
        }).replace('\'', '\\\'')
    py_interp = sys.executable
    app_command = f'{py_interp} main.py \'{cdktf_input}\''
    
    output_dir = output_dir or '.'
    
    with open("/tmp/test.log", "wb") as f:
        my_env = os.environ.copy()
        process = subprocess.Popen(["cdktf", "destroy", env_id,
                                    '--output', output_dir or '.',
                                    "--app", app_command], stdout=subprocess.PIPE, env=my_env)
        for c in iter(lambda: process.stdout.read(1), b''):
            sys.stdout.write(c.decode("utf-8", "ignore"))
            f.write(c)
    if process.poll():
        raise Exception

def _move_to_this_dir():
    # move to this folder as we need to be in cdktf context now
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

