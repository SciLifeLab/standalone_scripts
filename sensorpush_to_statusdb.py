"""Fetch sample readings from sensorpush API, summarize out-of-limit periods
   and upload those to StatusDB.

   Seems to be one reading per minute given by the API, so nr of samples can be
   seen as the number of minute fetched, for example 1440 samples for 24h.
"""

import requests
import argparse
import yaml
import os
import pytz
import datetime
import numpy as np
import pandas as pd
import logging
from couchdb import Server



class SensorPushConnection(object):
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self._authorized = False
        self.base_url = 'https://api.sensorpush.com/api/v1'
        self.access_token = None

    def _authorize(self):
        url_ending = 'oauth/authorize'
        url = '/'.join([self.base_url, url_ending])
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
        attempt = 1
        max_attempts = 3
        while attempt <= max_attempts:
            try:
                assert resp.status_code == 200
                attempt = 3
            except AssertionError:
                logger.error(f'Error fetching sensorpush data: {resp.text}, attempt {attempt} of {max_attempts}')
                if attempt > max_attempts:
                    resp.raise_for_status()
            attempt += 1
        return resp

    def get_samples(self, nr_samples, startTime=None, stopTime=None):
        url = '/samples'
        body_data = {
                'measures': ['temperature'],
            }
        if nr_samples:
            body_data['limit'] = nr_samples
        if startTime:
            body_data['startTime'] = startTime
        if stopTime:
            body_data['stopTime'] = stopTime

        r = self._make_request(url, body_data)
        return r.json()

    def get_sensors(self):
        url = '/devices/sensors'
        body_data = {}
        r = self._make_request(url, body_data)
        return r.json()


class SensorDocument(object):
    def __init__(self, original_samples, sensor_name, start_time, nr_samples_requested, limit_lower, limit_upper):
        self.original_samples = original_samples
        self.sensor_name = sensor_name
        self.start_time = start_time.strftime('%Y-%m-%dT%H:%M:%S')
        self.nr_samples_requested = nr_samples_requested
        self.limit_lower = limit_lower
        self.limit_upper = limit_upper
        self.intervals_lower = []
        self.intervals_lower_extended = []
        self.intervals_higher = []
        self.intervals_higher_extended = []

        # Save all samples around areas outside of limits, otherwise save only hourly averages
        self.saved_samples = {}

    def format_for_statusdb(self):
        return_d = vars(self)
        del return_d['original_samples']
        for interval_type in ['intervals_lower', 'intervals_lower_extended', 'intervals_higher', 'intervals_higher_extended']:
            return_d[interval_type] = self._interval_list_to_str(return_d[interval_type])
        # For convenience with the javascript plotting library, save it as a list of lists
        return_d['saved_samples'] = [[k, v] for k, v in sorted(return_d['saved_samples'].items())]
        return return_d

    def _interval_list_to_str(self, input_list):
        return [(sp.strftime('%Y-%m-%dT%H:%M:%S'), ep.strftime('%Y-%m-%dT%H:%M:%S')) for sp, ep in input_list]

    def _samples_from_intervals(self, intervals):
        for interval_lower, interval_upper in intervals:
            conv_lower = interval_lower.to_pydatetime()
            conv_upper = interval_upper.to_pydatetime()
            samples_dict = self.original_samples[conv_lower:conv_upper].to_dict()
            self.saved_samples.update({(k.strftime('%Y-%m-%dT%H:%M:%S'), round(v, 3)) for k, v in samples_dict.items()})

    def add_samples_from_intervals_lower(self):
        self._samples_from_intervals(self.intervals_lower_extended)

    def add_samples_from_intervals_higher(self):
        self._samples_from_intervals(self.intervals_higher_extended)

    def summarize_intervals(self, sample_series, limit_type):
        """Identify start- and endpoints of each out-of-limit intervals."""
        # Find all time points that are more than 2 minutes apart
        # closer than that and they will be considered the same interval
        gaps = np.abs(np.diff(sample_series.index)) > np.timedelta64(2, 'm')

        # Translate into positions
        gap_positions = np.where(gaps)[0] + 1

        interval_points = []
        extended_intervals = []
        for interval in np.split(sample_series, gap_positions):
            lower = interval.index[0]
            upper = interval.index[-1]
            interval_points.append((lower, upper))
            logger.warning(f'Interval with temperature too {limit_type} detected for {self.sensor_name} between: {lower} - {upper}')
            # Extended interval with 1 hour in each direction
            extend_lower = lower - np.timedelta64(1, 'h')
            extend_upper = upper + np.timedelta64(1, 'h')
            extended_intervals.append((extend_lower, extend_upper))

        return interval_points, extended_intervals

    def time_in_any_extended_interval(self, time_point):
        for interval_lower, interval_upper in self.intervals_lower_extended:
            if interval_lower < time_point < interval_upper:
                return True

        for interval_lower, interval_upper in self.intervals_higher_extended:
            if interval_lower < time_point < interval_upper:
                return True

        return False


def sensor_limits(sensor_info):
    limit_upper = None
    limit_lower = None
    temp_alerts = sensor_info['alerts'].get('temperature', {})

    if temp_alerts.get('enabled'):
        if 'max' in temp_alerts:
            limit_upper = to_celsius(temp_alerts['max'])
        if 'min' in temp_alerts:
            limit_lower = to_celsius(temp_alerts['min'])

    return limit_lower, limit_upper


def to_celsius(temp):
    return ((temp-32)*5)/9


def samples_to_df(samples_json):
    data_d = {}
    for sensor_id, samples in samples_json['sensors'].items():
        sensor_d = {}
        for sample in samples:
            time_point = datetime.datetime.strptime(sample['observed'], '%Y-%m-%dT%H:%M:%S.%fZ')
            # Make datetime aware of timezone
            time_point = time_point.replace(tzinfo=datetime.timezone.utc)
            # Transform to local timezone
            time_point = time_point.astimezone()
            sensor_d[time_point] = to_celsius(sample['temperature'])
        data_d[sensor_id] = sensor_d

    df = pd.DataFrame.from_dict(data_d)
    df = df.sort_index(ascending=True)
    return df


def process_data(sensors_json, samples_json, start_time, nr_samples_requested):
    df = samples_to_df(samples_json)

    sensor_documents = []
    for sensor_id, sensor_info in sensors_json.items():
        sensor_limit_lower, sensor_limit_upper = sensor_limits(sensor_info)
        if (sensor_limit_lower is None) and (sensor_limit_upper is None):
            logger.warning(f'Temperature alert not set for sensor {sensor_info["name"]}')

        sensor_samples = df[sensor_id].dropna()

        # TODO, samples are in Fahrenheit and UTC
        sd = SensorDocument(sensor_samples, sensor_info['name'], start_time,
                            nr_samples_requested, sensor_limit_lower, sensor_limit_upper)

        # Check if there are samples outside of limits
        if sensor_limit_lower is not None:
            samples_too_low = sensor_samples[sensor_samples < sensor_limit_lower]
            # Collect the exact intervals outside of limits
            if not samples_too_low.empty:
                sd.intervals_lower, sd.intervals_lower_extended = sd.summarize_intervals(samples_too_low, 'low')
                sd.add_samples_from_intervals_lower()

        if sensor_limit_upper is not None:
            samples_too_high = sensor_samples[sensor_samples > sensor_limit_upper]
            # Collect the exact intervals outside of limits
            if not samples_too_high.empty:
                sd.intervals_higher, sd.intervals_higher_extended = sd.summarize_intervals(samples_too_high, 'high')
                sd.add_samples_from_intervals_higher()

        hourly_mean = sensor_samples.resample('1H').mean()
        for hour, mean_val in hourly_mean.iteritems():
            # Don't add any hourly mean values where we've saved more detailed info
            if not sd.time_in_any_extended_interval(hour):
                sd.saved_samples[hour.strftime('%Y-%m-%dT%H:%M:%S')] = round(mean_val, 3)

        sensor_documents.append(sd)
    return sensor_documents


def main(nr_samples_requested, arg_start_date, statusdb_config, sensorpush_config, push):
    if arg_start_date is None:
        midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        start_date_datetime = midnight - datetime.timedelta(days=1)
    else:
        start_date_datetime = datetime.datetime.strptime(arg_start_date, '%Y-%m-%d')

    end_date_datetime = start_date_datetime + datetime.timedelta(days=1)

    # Need to use UTC timezone for the API call
    start_time = start_date_datetime.astimezone(tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_time = end_date_datetime.astimezone(tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    with open(os.path.expanduser(sensorpush_config), 'r') as sp_config_file:
        sp_config = yaml.safe_load(sp_config_file)

    if ('email' not in sp_config) or ('password' not in sp_config):
        raise Exception('Credentials missing in SensorPush config')

    sp = SensorPushConnection(sp_config['email'], sp_config['password'])

    logging.info(f'Fetching {nr_samples_requested} samples from {start_time}')
    # Request sensor data
    sensors = sp.get_sensors()
    samples = sp.get_samples(nr_samples_requested, startTime=start_time, stopTime=end_time)

    # Summarize data and put into documents suitable for upload
    sensor_documents = process_data(sensors, samples, start_date_datetime, nr_samples_requested)

    # Upload to StatusDB
    with open(statusdb_config) as settings_file:
        server_settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    url_string = 'http://{}:{}@{}:{}'.format(
                    server_settings['statusdb'].get('username'),
                    server_settings['statusdb'].get('password'),
                    server_settings['statusdb'].get('url'),
                    server_settings['statusdb'].get('port')
                )
    couch = Server(url_string)
    sensorpush_db = couch['sensorpush']

    for sd in sensor_documents:
        sd_dict = sd.format_for_statusdb()

        if push:
            logging.info(f'Saving {sd_dict["sensor_name"]} to statusdb')
            sensorpush_db.save(sd_dict)
        else:
            logging.info(f'Printing {sd_dict["sensor_name"]} to stderr')
            print(sd_dict)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', '-s', type=int, default=1440,
                        help=('Nr of samples that will be fetched'
                              'default value is 1440 e.g. 24 hours, '
                              'which is the maximum allowed value as well.')
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
    parser.add_argument('--logfile', '-l', default='~/log/sensorpush_script/to_statusdb.log',
                        help='Logfile used')
    parser.add_argument('--push', '-p', action='store_true',
                        help='Push to statusdb, otherwise just print to terminal'
                        )

    args = parser.parse_args()

    logging.basicConfig(
        filename=args.logfile, level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
        )

    logger = logging.getLogger(__name__)

    # Handler that will log warnings or worse to stderr
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.ERROR)
    logger.addHandler(stderr_handler)
    # Genomics status wants 1 document per day, so this is the way to enforce that,
    # should potentially be fixed in the future
    assert args.samples <= 1440

    main(args.samples, args.start_date, args.statusdb_config, args.config, args.push)
