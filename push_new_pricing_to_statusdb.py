#!/usr/bin/env python
"""
Reads the cost_calculator excel sheet and puts all that information
into statusdb.
"""
import argparse
from openpyxl import load_workbook
import coloredlogs
import logging
import yaml
from couchdb import Server
import datetime

FIRST_ROW = {'components': 9,
             'products': 4}
SHEET = {'components': 'Price list',
         'products': 'Products'}

# Skip columns which are calculated from the other fields
SKIP = {'components': ['Price', 'Total', 'Per unit']}

MAX_NR_ROWS = 200

# Set up a logger with colored output
logger = logging.getLogger(__name__)
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')

def is_empty_row(comp):
    for k,v in comp.items():
        if v != '':
            return False
    return True

def load_components(wb):
    ws = wb[SHEET['components']]

    # Unkown number of rows
    row = FIRST_ROW['components']
    header_row = row - 1
    header_cells = ws[header_row]
    header = {}
    for cell in header_cells:
        cell_val = cell.value
        if cell_val == 'ID':
            cell_val = 'REF_ID' # Don't want to confuse it with couchdb ids
        if cell_val not in SKIP['components']:
            # Get cell column as string
            cell_column = cell.coordinate.replace(str(header_row), '')
            header[cell_column] = cell_val

    components = {}
    while row < MAX_NR_ROWS:
        new_component = {}
        for col, header_val in header.items():
            val = ws["{}{}".format(col,row)].value
            if val is None:
                val = ''
            new_component[header_val] = val

        if not is_empty_row(new_component):
            components[new_component['REF_ID']] = new_component
        row += 1

    return components


def main(input_sheet, config, user, user_email,
         add_components=False, add_products=False, push=False):
    with open(config) as settings_file:
        server_settings = yaml.load(settings_file)
    couch = Server(server_settings.get("couch_server", None))

    wb = load_workbook(input_sheet, read_only=True, data_only=True)

    if add_components:
        db = couch['pricing_components']
        components = load_components(wb)
        doc = {}
        doc['components'] = components
        doc['Issued by user'] = user
        doc['Issued by user email'] = user_email
        doc['Issued at'] = datetime.datetime.now().isoformat()
        doc['Version'] = 2
        if push:
            db.save(doc)
        else:
            print(doc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pricing_excel_sheet',
                        help="The excel sheet currently used for pricing")
    parser.add_argument('--statusdb_config', required=True,
                        help='The genomics-status settings.yaml file.')
    parser.add_argument('--components', action='store_true',
                        help='Add the pricing components '
                        'from the "Price list" sheet.')
    parser.add_argument('--push', action='store_true',
                        help='Use this tag to actually push to the databse,'
                        ' otherwise it is just dryrun')
    parser.add_argument('--user', required=True,
                        help='User that change the document')
    parser.add_argument('--user_email', required=True,
                        help='Email used to tell who changed the document')
    args = parser.parse_args()

    main(args.pricing_excel_sheet, args.statusdb_config, args.user,
         args.user_email, add_components=args.components, push=args.push)
