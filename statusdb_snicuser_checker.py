#!/usr/bin/env python
"""
Get open projects from statusdb, checks if the users have a SNIC account and
writes the result back into statusdb.
"""
import argparse
import yaml
from couchdb import Server
import requests
from requests.auth import HTTPBasicAuth
import json

def update_statusdb(config, dryrun=True):
    url_string = 'http://{}:{}@{}:{}'.format(config['statusdb'].get('username'), config['statusdb'].get('password'),
                                              config['statusdb'].get('url'), config['statusdb'].get('port'))
    couch = Server(url=url_string)
    assert couch, 'Could not connect to {}'.format(settings.get('url'))

    proj_db = couch['projects']
    open_projs = proj_db.view('project/summary',include_docs=True, descending=True)[['open','Z']:['open','']]

    for project in open_projs:
        doc = project.doc
        flag = False
        if project.value.get('delivery_type') == 'GRUS':
            if project.value['details'].get('snic_checked'):
                if not project.value['details']['snic_checked']['status']:
                    email = project.value['order_details']['fields'].get('project_pi_email')
                    check = snic_check(email, config['SNIC'])
                    if check:
                        doc['details']['snic_checked']['status'] = check
                        flag = True

            else:
                snic_checked = {'status': True}
                #roles = ['project_lab_email','project_bx_email', 'project_pi_email']
                if project.value.get('order_details'):
                    email = project.value['order_details']['fields'].get('project_pi_email')
                    if email:
                        if not snic_check(email, config['SNIC']):
                            snic_checked['status'] = False
                    #Add the new field to project details
                    doc['details']['snic_checked'] = snic_checked
                    flag = True
        #write to projects doc
        if flag:
            if not dryrun:
                proj_db.save(doc)
                import pdb; pdb.set_trace()
            else:
                print(doc['project_name'], doc['details']['snic_checked'])

def snic_check(email, config):
    url = 'https://supr.snic.se/api/person/email_present/?email={}'.format(email)
    response = requests.get(url, auth=HTTPBasicAuth(config.get('username'), config.get('password')))
    return json.loads(response.content)['email_present']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', metavar='Path to config file', help='Path to yaml file with credentials for statusdb and SNIC API')
    parser.add_argument('--check_email', metavar='Option to run script to check emails', nargs = 1,
                        help='Check an individual email directly in SNIC')
    parser.add_argument('--update_statusdb',action='store_true', dest='update_statusdb', default=False,
                        help='Update SNIC status for users of all open projects in statusdb.')
    parser.add_argument('-d', '--dryrun',
                      action='store_true', dest='dryrun', default=False,
                      help='Use this to print out what would have been saved to statusdb')

    args = parser.parse_args()
    config = {}
    with open(args.config) as config_file:
        yamlfile = yaml.load(config_file, Loader=yaml.SafeLoader)
    config['statusdb'] = yamlfile['statusdb']
    config['SNIC'] = yamlfile['SNIC']
    if args.check_email:
        result = snic_check(args.check_email[0], config['SNIC'])
        print('The email "{}" has {} associated SNIC account.'.format(args.check_email[0], 'an' if result else 'NO'))
    elif args.update_statusdb:
        update_statusdb(config, args.dryrun)
