import os

import yaml

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = 'config.yml'
CONFIG_PATH = os.path.join(CURR_DIR, CONFIG_FILE)

__config__ = yaml.safe_load(open(CONFIG_PATH))
