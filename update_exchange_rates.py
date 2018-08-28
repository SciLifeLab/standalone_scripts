#!/usr/bin/env python
"""A script to update the exchange rates for 1 EUR in SEK and 1 USD om SEK.

"""

import argparse
import requests
import re
import yaml
from couchdb import Server
import datetime


def main(config, push_to_server=False):
    resp = requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml")
    if not resp.ok:
        raise Exception("Got a non-ok return code: {} from ECB. ABORTING".format(resp.status_code))

    # Look for the four lines we need using regexes.
    # I guess normal people would use an xml-parser
    for line in resp.text.split('\n'):
        date_r = r".*time='([0-9\-]*).*'"
        date_match = re.match(date_r, line)
        if date_match:
            date = re.findall(date_r, line)[0]

        name_r = r'.*:name>(.*)<.*'
        name_match = re.match(name_r, line)
        if name_match:
            source_name = re.findall(name_r, line)[0]

        usd_match = re.match(r".*currency='USD'.*", line)
        if usd_match:
            rate_match = re.findall(r".*rate='([0-9\.]*)'.*", line)
            eur_to_usd = float(rate_match[0])

        sek_match = re.match(r".*currency='SEK'.*", line)
        if sek_match:
            rate_match = re.findall(r".*rate='([0-9\.]*)'.*", line)
            eur_to_sek = float(rate_match[0])

    usd_to_sek = round(eur_to_sek/eur_to_usd, 4)

    # Create the doc that will be uploaded
    doc = {}
    doc['Issued at'] = datetime.datetime.now().isoformat()
    doc['Data source date'] = date
    doc['Data source'] = source_name
    doc['USD_in_SEK'] = usd_to_sek
    doc['EUR_in_SEK'] = eur_to_sek

    if push_to_server:
        with open(config) as settings_file:
            server_settings = yaml.load(settings_file)
        couch = Server(server_settings.get("couch_server", None))
        db = couch['pricing_exchange_rates']

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
