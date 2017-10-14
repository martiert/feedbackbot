import json
import argparse
import os

import feedback


parser = argparse.ArgumentParser()
default_config_file = '{}/.config/feedback/config.json'.format(os.path.expanduser('~'))
parser.add_argument(
    '--config',
    '-c',
    default=default_config_file,
    help='Path to configuration file. Default: {}'.format(default_config_file)
)

args = parser.parse_args()

with open(args.config, 'r') as fd:
    config = json.load(fd)

bot = feedback.Feedback(config)
bot.run()
