"""Fetch sample readings from sensorpush API, summarize out-of-limit periods
   and upload those to StatusDB.

   Seems to be one reading per minute given by the API, so nr of samples can be
   seen as the number of minute fetched, for example 1440 samples for 24h.
"""

import requests
import argparse
import yaml
import os
import sys
import pytz
from datetime import datetime, time, timedelta
import dateutil

class SensorPushConnection(object):
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self._authorized = False
        self.base_url = 'https://api.sensorpush.com/api/v1/'
        self.access_token = None

    def _authorize(self):
        url_ending = 'oauth/authorize'
        url = '/'.join(x.strip('/') for x in [self.base_url, url_ending] if x)
        body_data = {
                        'email': self.email,
                        'password': self.password
                    }
        resp = requests.post(url, json=body_data)

        assert resp.status_code == 200
        authorization_value = resp.json().get('authorization')
        body_data = {'authorization': "{}".format(authorization_value)}
        url_ending = 'oauth/accesstoken'
        url = '/'.join(x.strip('/') for x in [self.base_url, url_ending] if x)
        resp = requests.post(url, json=body_data)
        assert resp.status_code == 200
        self.access_token = resp.json().get('accesstoken')
        self._authorized = True

    def _make_request(self, url_ending, body_data):
        if not self._authorized:
            self._authorize()

        url = '/'.join(x.strip('/') for x in [self.base_url, url_ending] if x)
        auth_headers = {'Authorization': self.access_token}
        resp = requests.post(url, json=body_data, headers=auth_headers)
        try:
            assert resp.status_code == 200
        except AssertionError:
            print(resp)
        return resp

    def get_samples(self, nr_samples, startTime=None):
        url = '/samples'
        body_data = {
                'measures': ['temperature'],
            }
        if nr_samples:
            body_data['limit'] = nr_samples
        if startTime:
            body_data['startTime'] = startTime
        r = self._make_request(url, body_data)
        return r.json()

    def get_sensors(self):
        url = '/devices/sensors'
        body_data = {}
        r = self._make_request(url, body_data)
        return r.json()

    def count_requests(self, nr_samples):
        start_time = datetime.datetime.now()

        runtime_seconds = 0
        loopcount = 0
        while runtime_seconds < 120:
            loopcount += 1
            if (loopcount % 20) == 0:
                print("Made {} requests".format(loopcount))
            runtime = datetime.datetime.now() - start_time
            runtime_seconds = runtime.total_seconds()

            minutes_ago_td = datetime.timedelta(minutes=loopcount)
            request_start_time = start_time - minutes_ago_td
            self.get_samples(nr_samples, startTime=request_start_time.isoformat())


def process_data(sensors, samples):
    pass

def main(samples, arg_start_date, statusdb_config, sensorpush_config):
    if arg_start_date is None:
        midnight = datetime.combine(datetime.today(), time.min)
        start_date_datetime = midnight - timedelta(days=1)
    else:
        start_date_datetime = datetime.strptime(arg_start_date, '%Y-%m-%d')

    start_date = start_date_datetime.astimezone(tz=pytz.utc).isoformat()

    with open(os.path.expanduser(sensorpush_config), 'r') as sp_config_file:
        sp_config = yaml.safe_load(sp_config_file)

    if ('email' not in sp_config) or ('password' not in sp_config):
        raise Exception('Credentials missing in SensorPush config')

    sp = SensorPushConnection(sp_config['email'], sp_config['password'])

    # Request sensor data
    sensors = sp.get_sensors()
    samples = sp.get_samples(samples, startTime=start_date)

    return sensors, samples

    # Collect time points outside of range
    data = process_data(sensors, samples)

    # Upload to StatusDB


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', '-s', type=int,
                        help=('Nr of samples that will be fetched'
                              'default value is 1440 e.g. 24 hours')
                        )
    parser.add_argument('--start_date', type=str, default=None,
                        help=('Collect samples starting from midnight at this date, '
                              'by default, yesterday is used.')
                        )
    parser.add_argument('--statusdb_config', default='~/conf/statusdb_cred.yaml',
                        help='StatusDB config file, default is ~/conf/statusdb_cred.yaml'
                        )
    parser.add_argument('--config', '-c', default='~/conf/sensorpush_cred.yaml',
                        help='Sensorpush credentials, default is ~/conf/sensorpush_cred.yaml'
                        )

    args = parser.parse_args()

    main(args.samples, args.start_date, args.statusdb_config, args.config)
