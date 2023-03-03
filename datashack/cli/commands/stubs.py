import click
from datashack.cli.settings import click_pass_settings
from datashack.utils.io import file_write_lines, mkdir, rmdir
import inquirer
from datashack.utils.console import console, error_console
from datashack.cli.core.loaders import PyLoader
import yaml
import os
import requests
from time import sleep
from enum import Enum
import json
from datashack.datashack_infra_app.triggers import apply_imported_packages, delete_imported_packages
from fastapi.encoders import jsonable_encoder


class Action(Enum):
    plan='plan'
    apply='apply'
    delete='delete'


def _stack_apply_plan_delete(settings, env, src_folder, action: Action):
    os.environ["DATASHACK_ENV"] = env

    console.log(f'Reading models from {src_folder}')
    resource_configs = PyLoader(src_folder).resource_configs

    env = env or settings.ds_env_id

    console.log(f'found {len(resource_configs)} resources...')

    env_output_dir = os.path.join(settings.home, env)
    mkdir(env_output_dir)

    resources_output_file = os.path.join(env_output_dir, 'resources.json')

    data = {
            'resources': [
                {
                    'resource_type': rc.resource_type,
                    'resource_json': rc.resource_json
                }
                for rc in resource_configs
            ]
        }
    with open(resources_output_file, 'w') as fp:
        json.dump(data, fp, indent=2)


    for rc in resource_configs:
        console.log(f' - {rc.resource_type}')

    
    try:
        account_id = '1'
        output_dir = os.path.join(env_output_dir, 'tf')
        mkdir(output_dir)
        if action == Action.apply:
            outputs = apply_imported_packages(
                account_id, env, jsonable_encoder(data['resources']), on_stdout_update=None, output_dir=output_dir, plan=False)
        elif action == Action.plan:
            outputs = apply_imported_packages(
                account_id, env, jsonable_encoder(data['resources']), on_stdout_update=None, output_dir=output_dir, plan=True)
        elif action == Action.delete:
            delete_imported_packages(
                account_id, env, jsonable_encoder(data['resources']), output_dir=output_dir)
            rmdir(output_dir)
        console.log('Done')
    except Exception as e:
        error_console.log(e)


@click.command()
@click.argument('src_folder')
@click.option('--env', default='dev')
@click_pass_settings
def plan(settings, src_folder: str, env: str):
    """
    see what real infrastracture your code will create
    """
    _stack_apply_plan_delete(settings, env, src_folder, action=Action.plan)

@click.command()
@click.argument('src_folder')
@click.option('--env', default='dev')
@click_pass_settings
def apply(settings, src_folder: str, env: str):
    """
    create real infrastructure from your code
    """
    _stack_apply_plan_delete(settings, env, src_folder, action=Action.apply)
    

@click.command()
@click.argument('src_folder')
@click.option('--env', default='dev')
@click_pass_settings
def destory(settings, src_folder: str, env: str):
    """
    destroy infrastructure
    """
    _stack_apply_plan_delete(settings, env, src_folder, action=Action.delete)