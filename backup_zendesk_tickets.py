import requests
import datetime
import time
import os
import yaml
import logging
import json

from zendesk import Zendesk
import click

@click.command()
@click.option('--config-file', help='Path to the config file', required=True, type=click.Path(exists=True))
@click.option('--days', help='Since how many days ago to backup tickets', default=30, type=int)
def backup(config_file, days):
    with open(config_file, 'r') as f:
        config = yaml.load(f)
    url = config.get('url')
    if url is None:
        logging.error("No 'url' in config_file: {}".format(config_file))
        return
    username = config.get('username')
    if username is None:
        logging.error("No 'username' in config_file: {}".format(config_file))
        return
    token = config.get('token')
    if token is None:
        logging.error("No 'token' in config_file: {}".format(config_file))
        return

    output_path = config.get('output_path')
    if output_path is None:
        logging.error("No 'output_path' in config_file: {}".format(config_file))
        return

    now = datetime.datetime.now()
    one_month_ago = now - datetime.timedelta(days=days) # get data for the last N days
    timestamp = time.mktime(one_month_ago.timetuple())
    auth = ('{}/token'.format(username), token)
    url = "{}/api/v2/incremental/tickets.json?start_time={}".format(url, timestamp)
    logging.info('Retrieving data from {}'.format(url))
    try:
        r = requests.get(url, auth=auth)
    except Exception, e:
        logging.error("Cannot retrieve requested data from url: {}".format(url))
        logging.error(e.message)
        return

    tickets = r.json().get('tickets')
    filename = "{}.bckp.json".format(now.strftime("%Y-%m-%d_%H-%M"))
    output_file = os.path.join(output_path, filename)
    logging.info('Recording data to the file {}'.format(output_file))

    if not os.path.exists(output_path):
        try:
            os.makedirs(output_path)
        except Exception, e:
            logging.error('Cannot create path: {}'.format(output_path))
            logging.error(e.message)
            return
    try:
        file = open(output_file, 'w+')
    except Exception, e:
        logging.error('Cannot open/create file: {}'.format(output_file))
        logging.error(e.message)
        return
    try:
        file.write(json.dumps(tickets))
    except Exception, e:
        logging.error('Cannot write to file: {}'.format(output_file))
        logging.error(e.message)


if __name__ == '__main__':
    backup()