"""Generates dictionaries for the load on the quota using 'uquota'. If a couchdb
is specified, the dictionaries will be sent there. Otherwise prints the dictionaries.
"""
import argparse
import datetime
import subprocess
from platform import node as host_name
from pprint import pprint
import couchdb



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Formats uquota information as "
        "a dict, and sends it to a given CouchDB."))
    parser.add_argument("--server", dest="server", action="store", help=("Address "
        "to the CouchDB server. Add authentication like this if needed: user:password@couchdb_server.domain"))
    parser.add_argument("--db", dest="db", action="store", default="uppmax",
        help="Name of the CouchDB database")

    args = parser.parse_args()

    current_time = datetime.datetime.now()
    uq = subprocess.Popen(["/sw/uppmax/bin/uquota", "-q"], stdout=subprocess.PIPE)
    output = uq.communicate()[0]

    projects = output.split("\n/proj/")[1:]

    project_list = []
    for proj in projects:
        project_dict = {"time": current_time.isoformat()}

        project = proj.strip("\n").split()
        project_dict["project"] = project[0]
        project_dict["usage (GB)"] = project[1]
        project_dict["quota limit (GB)"] = project[2]
        try:
            project_dict["over quota"] = project[3]
        except IndexError:
            # Projects that are not over quota will not have this field, because
            # uquota command only outputs a * in case the project account is over quota
            pass

        project_list.append(project_dict)

    if not args.server:
        pprint(project_list)
    else:
        couch = couchdb.Server(args.server)
        db = couch[args.db]
        for fs_dict in project_list:
            db.save(fs_dict)
