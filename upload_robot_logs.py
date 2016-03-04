"""
Script used to upload robot log messages to statusdb. Works either with stdin or a file. 

Denis Moreno, Joel Gruselius
Scilifelab
2016-03-04
"""
import argparse
import sys
import datetime
import couchdb
import yaml

def save_to_statusdb(db, message, args):
    data={'message':message}
    data['timestamp']=datetime.datetime.now().isoformat()
    data['instrument_name']=args.name

    db.save(data)


def read_message(args):
    if args.input_file:
        #read from input file
        with open(args.input_file, 'rb') as inp:
            message=inp.read()
    else:
        #read from stdin
        message=sys.stdin.read().rstrip()

    return message


def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)

def main(args):
    #read the configuration
    with open(args.conf) as conf_file:
        conf=yaml.load(conf_file)

    couch=setupServer(conf)
    db=couch['instrument_logs']

    message=read_message(args)
    save_to_statusdb(db, message, args)




if __name__=="__main__":
    usage = "Usage:       upload_robot_logs.py [options]"
    parser = argparse.ArgumentParser(description='Upload any message to statusdb.', usage=usage)

    parser.add_argument("-f", "--file", dest = "input_file", help = ("file",
                      " to read. its contents will be uploaded to satusdb as a message"))

    parser.add_argument("-n", "--name", dest = "name", help = (
                      "name of the instrument"))

    parser.add_argument("-l", "--logfile", dest = "logfile", help = ("log file",
                      " that will be used. default is ./statusdb_upload.log "), default="statusdb_upload.log")

    parser.add_argument("-c", "--conf", dest="conf", 
    default='statusdb.yaml', 
    help = "Config file.  Default: ./statusdb.yaml")

    args = parser.parse_args()
    main(args)

