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
from collections import OrderedDict

FIRST_ROW = {'components': 9,
             'products': 4}
SHEET = {'components': 'Price list',
         'products': 'Products'}

# Skip columns which are calculated from the other fields
SKIP = {'components': ['Price', 'Total', 'Per unit'],
        'products': ['Internal', 'External']}

# The name of the _id_ key and which variables that cannot be changed
# while keeping the same _id_. If an update of any of these fields is needed,
# a new id needs to be created.
CONSERVED_KEY_SETS = {'products': ('ID', ['Category', 'Type', 'Name']),
                      'components': ('REF_ID', ['Category', 'Type', 'Product name'])}

# The combination of these "columns" need to be unique within the document
UNIQUE_KEY_SETS = {'products': ('ID', ['Category', 'Type', 'Name']),
                   'components': ('REF_ID', ['Category', 'Type', 'Product name', 'Units'])}

MAX_NR_ROWS = 200

# Assuming the rows of products are sorted in the preferred order

# Set up a logger with colored output
logger = logging.getLogger('push_new_pricing_logger')
logger.propagate = False  # Otherwise the messages appeared twice
coloredlogs.install(level='INFO', logger=logger,
                    fmt='%(asctime)s %(levelname)s %(message)s')


def check_unique(items, type):
    """Make sure all items within _items_

    fulfill the uniqueness criteria according to the UNIQUE_KEY_SETS
    """
    key_val_set = set()
    for id, item in items.items():
        id_key, keys = UNIQUE_KEY_SETS[type]
        t = tuple(item[key] for key in keys)

        # Check that it is not already added
        if t in key_val_set:
            raise ValueError("Key combination {}:{} is included multiple "
                             "times in the {} sheet. "
                             "ABORTING.".format(keys, t, type))
        key_val_set.add(t)
    return True


def check_conserved(new_items, current_items, type):
    """Ensures the keys in CONSERVED_KEY_SETS are conserved for each given id.

    Compares the new version against the currently active one.
    Params:
        new_items     - A dict of the items that are to be added
                        with ID attribute as the key.
        current_items - A dict of the items currently in the database
                        with ID attribute as the key.
        type          - Either "components" or "products"
    """
    conserved_keys = CONSERVED_KEY_SETS[type][1]

    for id, new_item in new_items.items():
        if str(id) in current_items:
            for conserved_key in conserved_keys:
                if conserved_key not in new_item:
                    raise ValueError("{} column not found in new {} row with "
                                     "id {}. This column needs to be kept "
                                     "conserved. ABORTING!".format(
                                        conserved_key,
                                        type,
                                        id
                                        ))
                if new_item[conserved_key] != current_items[str(id)][conserved_key]:
                    raise ValueError("{} need to be conserved for {}."
                                     " Violated for item with id {}. "
                                     "Found {} for new and {} for current. "
                                     "ABORTING!".format(
                                        conserved_key,
                                        type,
                                        id,
                                        new_item[conserved_key],
                                        current_items[str(id)][conserved_key]
                                        ))
    return True

def get_current_items(db, type):
    rows = db.view("entire_document/by_version", descending=True, limit=1).rows
    if len(rows) != 0:
        doc = rows[0].value
        return doc[type]
    return {}


def is_empty_row(comp):
    for k, v in comp.items():
        if v != '':
            return False
    return True


def load_products(wb):
    ws = wb[SHEET['products']]

    row = FIRST_ROW['products']
    header_row = row - 1
    header_cells = ws[header_row]
    header = {}
    for cell in header_cells:
        cell_val = cell.value

        if cell_val not in SKIP['products']:
            # Get cell column as string
            cell_column = cell.coordinate.replace(str(header_row), '')
            header[cell_column] = cell_val

    products = OrderedDict()
    # Unkown number of rows
    while row < MAX_NR_ROWS:
        new_product = {}
        for col, header_val in header.items():
            val = ws["{}{}".format(col, row)].value
            if val is None:
                val = ''
            if header_val == 'Components':
                # Some cells might be interpreted as floats
                # e.g. "37,78"
                val = str(val)
                val = val.replace('.', ',')
                if val:
                    val_list = []
                    for comp_id in val.split(','):
                        try:
                            int(comp_id)
                        except ValueError:
                            print("Product on row {} has component with "
                                  "invalid id {}: not an integer, "
                                  " aborting!".format(row, comp_id))
                            raise
                        # Make a list with all individual components
                        val_list.append(comp_id)

                    val = {comp_ref_id: {'quantity': 1} for comp_ref_id in val_list}

            new_product[header_val] = val

        if not is_empty_row(new_product):
            product_row = row - FIRST_ROW['products'] + 1

            # The id seems to be stored as a string in the database
            # so might as well always have the ids as strings.

            product_row = str(product_row)

            # the row in the sheet is used as ID.
            # In the future this will have to be backpropagated to the sheet.
            products[product_row] = new_product
        row += 1

    return products


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
            cell_val = 'REF_ID'  # Don't want to confuse it with couchdb ids
        if cell_val not in SKIP['components']:
            # Get cell column as string
            cell_column = cell.coordinate.replace(str(header_row), '')
            header[cell_column] = cell_val

    components = {}
    while row < MAX_NR_ROWS:
        new_component = {}
        for col, header_val in header.items():
            val = ws["{}{}".format(col, row)].value
            if val is None:
                val = ''
            elif header_val == 'REF_ID':
                # The id seems to be stored as a string in the database
                # so might as well always have the ids as strings.
                try:
                    int(val)
                except ValueError:
                    print("ID value {} for row {} is not an id, "
                          "aborting.".format(val, row))
                val = str(val)

            new_component[header_val] = val

        if new_component['REF_ID'] in components:
            # Violates the uniqueness of the ID
            raise ValueError("ID {} is included multiple "
                             "times in the {} sheet. "
                             "ABORTING.".format(new_component['REF_ID'], type))

        if not is_empty_row(new_component):
            components[new_component['REF_ID']] = new_component
        row += 1

    return components


def get_current_version(db):
    view_result = db.view('entire_document/by_version', limit=1,
                          descending=True)
    if view_result.rows:
        return int(view_result.rows[0].value['Version'])
    else:
        return 0


def compare_two_objects(obj1, obj2, ignore_updated_time=True):
    # Make copies in order to ignore fields
    obj1_copy = obj1.copy()
    obj2_copy = obj2.copy()

    if ignore_updated_time:
        if 'Last Updated' in obj1_copy:
            obj1_copy.pop('Last Updated')
        if 'Last Updated' in obj2_copy:
            obj2_copy.pop('Last Updated')

    return obj1_copy == obj2_copy


def set_last_updated_field(new_objects, current_objects, object_type):
    # if object is not found or changed in current set last updated field
    now = datetime.datetime.now().isoformat()
    for id in new_objects.keys():
        updated = False
        if id in current_objects:
            # Beware! This simple == comparison is quite brittle. Sensitive to
            # str vs int and such.
            the_same = compare_two_objects(new_objects[id],
                                           current_objects[id])
            if not the_same:
                updated = True
        else:
            updated = True

        if updated:
            print("Updating {}: {}".format(object_type, id))
            new_objects[id]['Last Updated'] = now
        else:
            new_objects[id]['Last Updated'] = current_objects[id]['Last Updated']

    return new_objects


def main(input_file, config, user, user_email,
         add_components=False, add_products=False, push=False):
    with open(config) as settings_file:
        server_settings = yaml.load(settings_file)
    couch = Server(server_settings.get("couch_server", None))

    wb = load_workbook(input_file, read_only=True, data_only=True)

    if add_components:
        db = couch['pricing_components']
        components = load_components(wb)
        check_unique(components, 'components')

        current_components = get_current_items(db, 'components')

        # Otherwise the first version
        if current_components:
            check_conserved(components, current_components, 'components')

        # Modify the `last updated`-field of each item
        components = set_last_updated_field(components,
                                            current_components,
                                            'component')

        doc = {}
        doc['components'] = components
        doc['Issued by user'] = user
        doc['Issued by user email'] = user_email
        doc['Issued at'] = datetime.datetime.now().isoformat()

        current_version = get_current_version(db)
        doc['Version'] = current_version + 1

        if push:
            logger.info(
                'Pushing components document version {}'.format(doc['Version'])
                )
            db.save(doc)
        else:
            print(doc)

    if add_products:
        db = couch['pricing_products']
        products = load_products(wb)

        check_unique(products, 'products')

        current_products = get_current_items(db, 'products')

        # Otherwise the first version
        if current_products:
            check_conserved(products, current_products, 'products')

        # Modify the `last updated`-field of each item
        products = set_last_updated_field(products,
                                          current_products,
                                          'product')

        doc = {}
        doc['products'] = products
        doc['Issued by user'] = user
        doc['Issued by user email'] = user_email
        doc['Issued at'] = datetime.datetime.now().isoformat()

        current_version = get_current_version(db)
        doc['Version'] = current_version + 1

        if push:
            logger.info(
                'Pushing products document version {}'.format(doc['Version'])
                )
            db.save(doc)
        else:
            print(doc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pricing_excel_file',
                        help="The excel file currently used for pricing")
    parser.add_argument('--statusdb_config', required=True,
                        help='The genomics-status settings.yaml file.')
    parser.add_argument('--components', action='store_true',
                        help='Add the pricing components '
                        'from the "Price list" sheet.')
    parser.add_argument('--products', action='store_true',
                        help='Add the pricing products '
                        'from the sheet.')
    parser.add_argument('--push', action='store_true',
                        help='Use this tag to actually push to the databse,'
                        ' otherwise it is just dryrun')
    parser.add_argument('--user', required=True,
                        help='User that change the document')
    parser.add_argument('--user_email', required=True,
                        help='Email used to tell who changed the document')
    args = parser.parse_args()

    main(args.pricing_excel_file, args.statusdb_config, args.user,
         args.user_email, add_components=args.components,
         add_products=args.products, push=args.push)
