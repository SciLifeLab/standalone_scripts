#!/usr/bin/env python
"""A script to update the exchange rates for 1 EUR in SEK and 1 USD om SEK.

"""

import argparse
import yaml
from couchdb import Server
import datetime
from forex_python.converter import CurrencyRates


def get_current(db, view):
    rows = db.view("by_date/{}".format(view), descending=True, limit=1).rows
    if len(rows) != 0:
        value = rows[0].value
        return value
    return None

def main(config, push_to_server=False):

    c = CurrencyRates()
    # Will raise RatesNotAvailableError if not able to fetch from the api
    usd_to_sek = c.get_rate('USD', 'SEK')
    eur_to_sek = c.get_rate('EUR', 'SEK')

    # Create the doc that will be uploaded
    doc = {}
    doc['Issued at'] = datetime.datetime.now().isoformat()
    # I know it's bad practice to call the _source_url method,
    # but if it breaks it breaks.
    doc['Data source'] = "forex_python ({})".format(c._source_url())
    doc['USD_in_SEK'] = usd_to_sek
    doc['EUR_in_SEK'] = eur_to_sek

    # Load the statusdb database
    with open(config) as settings_file:
        server_settings = yaml.load(settings_file)
    couch = Server(server_settings.get("couch_server", None))
    db = couch['pricing_exchange_rates']

    # Check that new is not too strange compared to current.
    # This is a safety measure so that we have lower risk of having erroneus
    # exchange rates in the db.
    current_usd = get_current(db, 'usd_to_sek')
    if current_usd is not None:
        rel_change = (usd_to_sek-current_usd)/current_usd
        print("INFO: Change in USD exchange rate: {:.3f}%".format(100*(rel_change)))

        if abs(rel_change) > 0.20:
            raise Exception("Financial crisis or rather; something is likely wrong!")

    current_eur = get_current(db, 'eur_to_sek')
    if current_eur is not None:
        rel_change = (eur_to_sek-current_eur)/current_eur
        print("INFO: Change in EUR exchange rate: {:.3f}%".format(100*(rel_change)))

        if abs(rel_change) > 0.20:
            raise Exception("Financial crisis or rather; something is likely wrong!")

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
                        help='The genomics-status settings.yaml file.')
    parser.add_argument('--push', action='store_true', help='Use this tag to '
                        "make the script push the changes to statusdb")

    args = parser.parse_args()
    main(args.statusdb_config, push_to_server=args.push)
