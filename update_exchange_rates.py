#!/usr/bin/env python
"""A script to update the exchange rates for 1 EUR in SEK and 1 USD in SEK.

"""

import argparse
from couchdb import Server
import datetime
import json
import requests
import yaml


class CurrencyRates(object):
    """A class to fetch currency rates from fixer.io."""
    def __init__(self, config_file):

        self.rates_fetched = False
        self._source_url = "https://api.apilayer.com/fixer/latest"

        with open(config_file, 'r') as fh:
            config = yaml.load(fh, Loader=yaml.SafeLoader)
        self._apikey = config.get('apikey')

    def fetch_rates(self):
        response = requests.get(self._source_url, params={'base': 'SEK', 'symbols': 'USD, EUR'}, headers={'apikey': self._apikey})
        assert response.status_code == 200
        self.data = json.loads(response.text)
        self.rates = self.data['rates']
        self.rates_fetched = True

    def get_rate(self, currency):
        """Get the exchange rate for SEK to the given currency."""
        if not self.rates_fetched:
            self.fetch_rates()

        return 1/self.rates[currency]


def get_current(db, item):
    rows = db.view("entire_document/by_date", descending=True, limit=1).rows
    if len(rows) != 0:
        value = rows[0].value
        return value[item]
    return None

def check_financial_crisis(current_val, new_val, currency):
    if current_val is not None:
        rel_change = (new_val-current_val)/current_val
        print("INFO: Change in {} "
              "exchange rate: {:.3f}%".format(currency, 100*(rel_change)))

        if abs(rel_change) > 0.20:
            raise Exception("Financial crisis or rather; something is likely wrong!")

def main(config, fixer_io_config, push_to_server=False):

    c = CurrencyRates(fixer_io_config)
    # Will raise RatesNotAvailableError if not able to fetch from the api
    usd_to_sek = c.get_rate('USD')
    eur_to_sek = c.get_rate('EUR')

    # Inconsistent results for Euro after broken API was updated
    if isinstance(eur_to_sek, str):
        eur_to_sek = float(eur_to_sek)

    # Create the doc that will be uploaded
    doc = {}
    doc['Issued at'] = datetime.datetime.now().isoformat()
    # I know it's bad practice to call the _source_url method,
    # but if it breaks it breaks.
    doc['Data source'] = "Fixer.io via ({})".format(c._source_url)
    doc['USD_in_SEK'] = usd_to_sek
    doc['EUR_in_SEK'] = eur_to_sek

    # Load the statusdb database
    with open(config) as settings_file:
        server_settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    url_string = 'https://{}:{}@{}'.format(
                    server_settings['statusdb'].get('username'),
                    server_settings['statusdb'].get('password'),
                    server_settings['statusdb'].get('url')
                )
    couch = Server(url_string)

    db = couch['pricing_exchange_rates']

    # Check that new is not too strange compared to current.
    # This is a safety measure so that we have lower risk of having erroneus
    # exchange rates in the db.
    current_usd = get_current(db, 'USD_in_SEK')
    current_eur = get_current(db, 'EUR_in_SEK')

    check_financial_crisis(current_usd, usd_to_sek, 'USD')
    check_financial_crisis(current_eur, eur_to_sek, 'EUR')

    # Completely conserved currencies are also strange.
    if (current_eur is not None) and (current_usd is not None):
        # This assumes the script is not ran too often
        # (api udpates once per day)
        small_number = 0.0000000001
        if (abs(current_usd - usd_to_sek) < small_number) and \
                (abs(current_eur - eur_to_sek) < small_number):
            raise Exception("Super stable currencies? Stale api would be my guess.")

    if push_to_server:
        db.save(doc)
    else:
        print(doc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--statusdb_config', required=True,
                        help='The statusdb_cred.yaml file.')
    parser.add_argument('--fixer_io_config', required=True,
                        help='The fixer_io.yaml file.')
    parser.add_argument('--push', action='store_true', help='Use this tag to '
                        "make the script push the changes to statusdb")

    args = parser.parse_args()
    main(args.statusdb_config, args.fixer_io_config, push_to_server=args.push)
