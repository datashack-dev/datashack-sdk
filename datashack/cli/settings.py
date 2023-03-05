import os
import click
from datashack.cli.consts import Consts
from datashack.cli.core.os import OS
from datashack.utils.io import mkdir
from os import environ as env
import platform


class Settings:
    def __init__(self, home=None, terminal=None):
        self.home = os.path.abspath(home or Consts.HOME)
        self.terminal = terminal or Consts.DEFAULT_TERMINAL
        mkdir(self.home)

        self.os = self.get_os()

        self.ds_env_id = env.get('DATASHACK_env_id')
        self.ds_zone_id = env.get('DATASHACK_ZONE_ID')

    def get_os(self)->OS:
        try:
            return OS.from_str(platform.system().lower())
        except:
            return OS.ANY
        
# from https://click.palletsprojects.com/en/8.1.x/complex/
click_pass_settings = click.make_pass_decorator(Settings, ensure=True)
