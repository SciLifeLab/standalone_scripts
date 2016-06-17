import socket
import os
import couchdb
import glob
import re
import sys
import logging
from datetime import datetime
from datetime import date
import dateutil.parser
import argparse
import ConfigParser
import yaml
from sets import Set
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

CONFIG = {}

logger = logging.getLogger(__name__)



def main(args):
    configuration_file = args.config
    load_yaml_config(configuration_file)
    
    if args.dump_db_status:
        current_status  = fetch_current_status_from_db()
        save_status_on_yaml(current_status, args.save)
    
    if args.create_new_status:
        old_status      = fetch_status_from_yaml(args.status)
        current_status  = fetch_current_status_from_db()
        new_status      = update_status(old_status, current_status)
        save_status_on_yaml(new_status, args.save)

    if args.fetch_gdocs_status:
        gdocs_status = fetch_status_from_gdocs()
        save_status_on_yaml(gdocs_status, args.save)

    if args.save_to_gdocs:
        save_status_to_gdocs(args.status)




def fetch_status_from_gdocs():
    scope = CONFIG['gdocs']['g_scope']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["gdocs"]["credentials"], scope)
    gc = gspread.authorize(credentials)
    #now connect to the sheet
    sheet = gc.open(CONFIG['gdocs']['g_sheet'])
    #now fetch the latest worksheet
    worksheet = sheet.worksheets()[-1]
    #conditions to check in col 1:
    # --  means a new record follows
    # END end of doc
    #start by the beginning
    row = 1
    col = 1
    cell_value = worksheet.cell(row, col).value
    status = {}
    while cell_value != "END":
        if cell_value == "----":
            #in this case go to next row and start to ead the project info
            row += 1
            project_id = worksheet.cell(row, col).value
            col = 3 # now I am reading the fixed records
            row += 1
            project_name = worksheet.cell(row, col).value
            row += 1
            comment = worksheet.cell(row, col).value
            row += 1
            application = worksheet.cell(row, col).value
            row += 1
            responsible = worksheet.cell(row, col).value
            row += 1 # header "runs samples comment
            row += 1
            runs = {} #runs status
            while  worksheet.cell(row, 1).value != "----" and worksheet.cell(row, 1).value != "END":
                run_id      = worksheet.cell(row, 2).value
                num_samples = worksheet.cell(row, 3).value
                comment     = worksheet.cell(row, 4).value
                row += 1
                runs[run_id] = {"samples" : num_samples,
                               "comment" : comment
                               }
            #now create status entry
            status[project_id] =  {"project_name" : project_name,
                                    "application" : application,
                                    "comment"     : comment,
                                    "responsible" : responsible,
                                    "runs": runs
                                    }
        else:
            print "ERROR: something wrong in how the preadsheet is set up while processing row {}, column {}".format(row, col)
        col = 1
        cell_value = worksheet.cell(row, col).value
    return status




def save_status_to_gdocs(status_to_update):
    scope = CONFIG['gdocs']['g_scope']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["gdocs"]["credentials"], scope)
    gc = gspread.authorize(credentials)
    #now connect to the sheet
    sheet = gc.open(CONFIG['gdocs']['g_sheet'])
    worksheet_name = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # name of the sheet is the time in which I update it
    worksheet      = sheet.add_worksheet(worksheet_name, 2000, 10)
    #now load status to be saved
    status      = fetch_status_from_yaml(status_to_update)
    row = 1
    for project_id in sorted(status):
        worksheet.update_cell(row, 1, "----") #place holder for start of project
        row += 1 # skip a row
        col = 1 #reset columns
        worksheet.update_cell(row, col, project_id)
        row += 1
        col  = 2
        worksheet.update_cell(row, col, "project_name")
        col += 1
        worksheet.update_cell(row, col, status[project_id]["project_name"])
        row += 1 # next row
        col =  2 # reset col
        worksheet.update_cell(row, col, "comment")
        col += 1 #next col
        worksheet.update_cell(row, col, status[project_id]["comment"])
        row += 1 # next row
        col =  2 # reset col
        worksheet.update_cell(row, col, "application")
        col += 1 #next col
        worksheet.update_cell(row, col, status[project_id]["application"])
        row += 1 # next row
        col =  2 # reset col
        worksheet.update_cell(row, col, "responsible")
        col += 1 #next col
        worksheet.update_cell(row, col, status[project_id]["responsible"])
        row += 1 # next row
        col =  2 # reset col
        worksheet.update_cell(row, col    , "runs")
        worksheet.update_cell(row, col + 1, "samples")
        worksheet.update_cell(row, col + 2, "comment")
        row += 1 # next row
        for run_id in sorted(status[project_id]["runs"]):
            col = 2 # reset column
            worksheet.update_cell(row, col, run_id)
            col += 1
            worksheet.update_cell(row, col, status[project_id]["runs"][run_id]["samples"])
            col += 1
            worksheet.update_cell(row, col, status[project_id]["runs"][run_id]["comment"])
            row += 1
            
    worksheet.update_cell(row, 1, "END")

def update_status(old_status, current_status):
    """
        this function takes two object as input, one rapressenting a old status (with comments) the other the current status (without comments)
        maybe in the short future we will have a comment about coming samples.
        the result is a new object such that:
            1- P1 in old but not in current
                - P1 not in new has it has been closed
            2- P1 in old and in current with same runs:
                - new will contain same content of old
            3- P1 in old and in current but current as more (or less?) runs
               3a  - new will contain all old runs with comment and the new ones with comment "NEW"
               3b - in case there are runs present in old and not in current notify
            4- P1 not in old and in current:
                - new will contain P1, all runs marked as NEW and notify
    """
    #start to go through old projects
    new_status = {}
    for project_id in old_status:
        if project_id in current_status:
            #insert this into new (after updaiting it if we are in case 3)
            runs_old_set = set()
            for run in old_status[project_id]["runs"]:
                runs_old_set.add(run)
            runs_current_set = set()
            for run in current_status[project_id]["runs"]:
                runs_current_set.add(run)
            
            current_old_intersection = runs_current_set.intersection(runs_old_set)
            only_old                 = runs_old_set.difference(runs_current_set)
            only_current             = runs_current_set.difference(runs_old_set)
            #case 2:
            new_runs = {}
            for run in current_old_intersection:
                #runs in common need to copy old in new (to keep last comment)
                new_runs[run] = old_status[project_id]["runs"][run] # this will copy the old info
            for run in only_current:
                new_runs[run] = current_status[project_id]["runs"][run]
                new_runs[run]["comment"]     = "NEW"
            for run in only_old:
                new_runs[run] = old_status[project_id]["runs"][run]
                new_runs[run]["comment"] = "FOUND IN OLDER STATUS"
                print "WARNING: run {} for project {} found in old status but not in current. This is unexpected. Run has been added to new status".format(run, project_id)
            
            new_status[project_id] =  {"project_name" : old_status[project_id]["project_name"], #leave this unchange
                                        "application" : old_status[project_id]["application"],  #leave this unchange
                                        "comment"     : old_status[project_id]["comment"],
                                        "responsible" : old_status[project_id]["responsible"],
                                        "runs": new_runs #update this
                                        }
        else:
            #in case 1
            print "LOG: project {} present in old status but not in current. Proeject has been closed and will not be copied in new.".format(project_id)
    
    for project_id in current_status:
        # check that this is the first time we see it
        if project_id not in new_status:
            #then we are in case 4, add the project to new
            new_status[project_id] = current_status[project_id]
            new_status[project_id]["comment"] = "NEW"
            for run in new_status[project_id]["runs"]:
                new_status[project_id]["runs"][run]["comment"] = "NEW"
    

    return  new_status
    return 0

def fetch_status_from_yaml(snapshot_yaml):
    stream = file(snapshot_yaml, 'r')
    return yaml.load(stream)


def save_status_on_yaml(status, file_name):
    stream = file(file_name, 'w')
    yaml.safe_dump(status, stream)

    
def fetch_current_status_from_db():
    """
        this function list all open projects (open date but not close date) and list all their runs on x_flowcell_db
        in the future it will check runs bioinfo_tab to look for coming ones
    """
    couch=setupServer(CONFIG)
    projects_db=couch['projects']
    open_projects_and_runs = {}
    for project_key in projects_db:
        if "creation_time" not in projects_db[project_key]:
            continue
        #check the creation date
        creation_date = dateutil.parser.parse(projects_db[project_key]["creation_time"]).replace(tzinfo=None)
        #not intrested in nothing created before january first 2015
        date_limit    = datetime(2015, 1, 1)
        if creation_date > date_limit and "close_date" not in projects_db[project_key]:
            #I am looking for project that have not a close data (i.e. they are running)
            project_id   = projects_db[project_key]["project_id"]
            project_name = projects_db[project_key]["project_name"]
            application  = "unkonwn"
            if "application" in projects_db[project_key]:
                application = projects_db[project_key]["application"]
            #for now look only at these two kind of applications
            #if application != "WG re-seq (IGN)" and application != "WG re-seq":
            #    continue
            #open project, now check sequencing informations
            flowcell_db = couch["x_flowcells"]
            #now find all FCs associated to this project
            flowcells_lanes = {}
            for fc_doc in flowcell_db:
                try:
                    samplesheet = flowcell_db[fc_doc]["samplesheet_csv"]
                except KeyError:
                    continue
                for sample_lane in samplesheet:
                    #import pdb
                    #pdb.set_trace()
                    
                    try:
                        current_project = sample_lane["Sample_Name"].split("_")[0]
                        sample_name     = sample_lane["Sample_Name"]
                    except KeyError:
                        current_project = sample_lane["SampleName"].split("_")[0]
                        sample_name     = sample_lane["SampleName"]
                    if current_project == project_id:
                        #in this case I have to update the flowcells_lanes
                        run_id = flowcell_db[fc_doc]["RunInfo"]["Id"]
                        if run_id in flowcells_lanes:
                            flowcells_lanes[run_id].add(sample_name) # count number of samples ... not really...
                        else:
                            flowcells_lanes[run_id] = Set([sample_name])
            
            if len(flowcells_lanes) > 0:
                #print "{} -- {}".format(project_id, project_name)
                for flowcell in flowcells_lanes:
                    flowcells_lanes[flowcell] = {"samples" : len(flowcells_lanes[flowcell]) , "comment": ""} # store only the numebr of samples that this project had in this run
                    #print "{} {}".format(flowcell, len(flowcells_lanes[flowcell]))
                #create object that will be dump into yaml
                open_projects_and_runs[project_id] = {"project_name"  : project_name,
                                                        "comment"     : "",
                                                        "application" : application,
                                                        "responsible" : "",
                                                        "runs"        : flowcells_lanes}
    return open_projects_and_runs


def list_applications():
    couch        = setupServer(CONFIG)
    projects_db  = couch['projects']
    applications = {}
    for project_id in projects_db:
        if "creation_time" not in projects_db[project_id]:
            continue
        application  = "unkonwn"
        if "application" in projects_db[project_id]:
            application = projects_db[project_id]["application"]
        if application in applications:
            applications[application] += 1
        else:
            applications[application] = 1
    for application in applications:
        print "{} {}".format(application, applications[application])




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
    parser = argparse.ArgumentParser("""This scripts queries statusdb and fetch informaiton about open projects and their runs""")
    parser.add_argument('--config', help="configuration file", type=str,  required=True)
    parser.add_argument('--save', help="name of the yaml file where the output of the script is saved", type=str)
    
    parser.add_argument('--dump-db-status'     , help="if specified dumps current status from db and saves in yaml files specified in --save", action='store_true')
    parser.add_argument('--fetch-gdocs-status' , help="if specified dumps the gdocs content to a yaml file specified in --save", action='store_true')
    parser.add_argument('--save-to-gdocs'      , help="if specified stores --status into a new gdocs sheet (name is current time)" , action='store_true')
    parser.add_argument('--create-new-status'  , help="takes as input a yaml files storing an old status and update it with the current status from db and stores it in --save", action='store_true')
    parser.add_argument('--status'             , help="name of the yaml file where an old status is stored", type=str)
    
    args = parser.parse_args()
    
    if not args.dump_db_status and not args.fetch_gdocs_status and not  args.save_to_gdocs and not args.create_new_status:
        print "ERROR: --dump-db-status , --dump-gdocs-status , --save-to-gdocs, --create-new-status : one must be specified"
        sys.exit()
    
    if args.dump_db_status:
        #other mutual options need to be false
        if args.fetch_gdocs_status or args.save_to_gdocs or args.create_new_status:
            print "ERROR: --dump-db-status , --dump-gdocs-status , --save-to-gdocs, --create-new-status : one must be specified"
            sys.exit()
        if not args.save:
            "ERROR: --save is mandatory"
            sys.exit()

    if args.create_new_status:
        #other mutual options need to be false
        if args.fetch_gdocs_status or args.save_to_gdocs or args.dump_db_status:
            print "ERROR: --dump-db-status , --dump-gdocs-status , --save-to-gdocs, --create-new-status : one must be specified"
            sys.exit()
        if not args.status:
            print "ERROR: option --status needed"
            sys.exit()
        if not args.save:
            "ERROR: --save is mandatory"
            sys.exit()


    if args.fetch_gdocs_status:
        #other mutual options need to be false
        if args.dump_db_status or args.create_new_status or args.save_to_gdocs:
            print "ERROR: --dump-db-status , --dump-gdocs-status , --save-to-gdocs, --create-new-status : one must be specified"
            sys.exit()
        if not args.save:
            "ERROR: --save is mandatory"
            sys.exit()


    if args.save_to_gdocs:
        #other mutual options need to be false
        if args.dump_db_status or args.create_new_status or args.fetch_gdocs_status:
            print "ERROR: --dump-db-status , --dump-gdocs-status , --save-to-gdocs, --create-new-status : one must be specified"
            sys.exit()
        if not args.status:
            print "ERROR: --status needs to be specified when --save-to-gdocs is present"
            sys.exit()

    main(args)



