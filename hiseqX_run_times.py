import os
import couchdb
import glob
import logging
from datetime import datetime
from datetime import date
import argparse
import ConfigParser
import yaml

CONFIG = {}

logger = logging.getLogger(__name__)



def main(args):
    configuration_file = args.config
    load_yaml_config(configuration_file)
    couch=setupServer(CONFIG)
    flowcell_db = couch["x_flowcells"]
    instruments = {}
    instruments["ST-E00198"] = []
    instruments["ST-E00201"] = []
    instruments["ST-E00214"] = []
    instruments["ST-E00266"] = []
    instruments["ST-E00269"] = []

    for fc_doc in flowcell_db:
        try:
            instrument = flowcell_db[fc_doc]["Runinfo"]["Instrument"]
            fcid = flowcell_db[fc_doc]["Runinfo"]["Id"]
        except KeyError:
            if "RunInfo" in flowcell_db[fc_doc]:
                instrument = flowcell_db[fc_doc]["RunInfo"]["Instrument"]
                fcid = flowcell_db[fc_doc]["RunInfo"]["Id"]
            else:
                continue
        #check if the instrument is one of the ones I want to check
        if instrument in ["ST-E00198", "ST-E00201", "ST-E00214", "ST-E00266", "ST-E00269"]:
            try:
                time_cycles = flowcell_db[fc_doc]["time cycles"]
            except KeyError:
                continue
            first_cycle_start = time_cycles[0]['start']
            last_cycle_end = time_cycles[-1]['end']
            # the split is done to remove the decimal point in the seconds
            first_cycle_date = datetime.strptime(first_cycle_start.split(".")[0], '%Y-%m-%d %H:%M:%S')
            last_cycle_date = datetime.strptime(last_cycle_end.split(".")[0], '%Y-%m-%d %H:%M:%S')
            delta = last_cycle_date - first_cycle_date
            instruments[instrument].append({"{}".format(fcid):  delta.total_seconds()/3600 } )
            
    for instrument in instruments:
        print "time\t{}".format(instrument)
        for run in  sorted(instruments[instrument]):
            date_illumina_format =run.keys()[0].split("_")[0]
            date_exel_format="{}/{}/20{}".format(date_illumina_format[4:6] , date_illumina_format[2:4], date_illumina_format[0:2])
            print "{}\t{}".format(date_exel_format, run[run.keys()[0]])




def setupServer(conf):
    db_conf = conf['statusdb']
    url="http://{0}:{1}@{2}:{3}".format(db_conf['username'], db_conf['password'], db_conf['url'], db_conf['port'])
    return couchdb.Server(url)



def load_yaml_config(config_file):
    """Load YAML config file

    :param str config_file: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    if type(config_file) is file:
        CONFIG.update(yaml.load(config_file) or {})
        return CONFIG
    else:
        try:
            with open(config_file, 'r') as f:
                content = yaml.load(f)
                CONFIG.update(content)
                return content
        except IOError as e:
            e.message = "Could not open configuration file \"{}\".".format(config_file)
            raise e



if __name__ == '__main__':
    parser = argparse.ArgumentParser("""Check running times""")
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    args = parser.parse_args()
    
    main(args)



