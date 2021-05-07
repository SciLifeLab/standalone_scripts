
""" Calls up the genologics LIMS directly in order to more quickly
    set a bioinformatics responsible. Script can easily be altered
    to be used to set other values."""

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Udfconfig

def namesetter(PID):

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    #Find LIMS entry with same PID
    allProjects = lims.get_projects()
    for proj in allProjects:
        if proj.id == PID:
            limsproject = proj.name
            break
    #Error handling
    if not 'limsproject' in locals():
        print("{} not available in LIMS.".format(PID))
        return None

    #Enter project summary process
    stepname=['Project Summary 1.3']
    process=lims.get_processes(type=stepname, projectname=limsproject)
    #Error handling
    if process == []:
        print("{} for {} is not available in LIMS.".format(stepname, limsproject))
        return None

    loop = True
    while loop:
        if "Bioinfo responsible" in process[0].udf:
            response = process[0].udf["Bioinfo responsible"]
        else:
            response = "Unassigned"
        print("Existing Bioinfo responsible for project {} aka {} is: {}".format(limsproject, PID, response.encode('utf-8')))

        #Checks for valid name
        in_responsibles = False
        config_responsibles =Udfconfig(lims, id="1128")
        while not in_responsibles:
            if sys.version_info[0] == 3:
                newname = input("Enter name of new Bioinfo responsible: ")
            elif sys.version_info[0] == 2:
                newname = raw_input("Enter name of new Bioinfo responsible: ")
            for names in config_responsibles.presets:
                if newname in names:
                    in_responsibles = True
                    newname = names
            if not in_responsibles:
                print("Subset {} not found in accepted Bioinfo responsible list.".format(newname))
            else:
                print("Suggested name is {}".format(newname))

        if sys.version_info[0] == 3:
            confirmation = input("Project {} aka {} will have {} as new Bioinfo responsible, is this correct (Y/N)? ".format(limsproject, PID, newname))
        elif sys.version_info[0] == 2:
            confirmation = raw_input("Project {} aka {} will have {} as new Bioinfo responsible, is this correct (Y/N)? ".format(limsproject, PID, newname))
        if confirmation == 'Y' or confirmation == 'y':
            try:
                newname.encode('ascii')
                process[0].udf["Bioinfo responsible"] = str(newname)
                process[0].put()
                print("Project {} aka {} assigned to {}".format(limsproject, PID, newname))
                return None
            except (UnicodeDecodeError, UnicodeEncodeError):
                #Weird solution due to put function
                process[0].udf["Bioinfo responsible"] = response
                print("ERROR: You tried to use a special character, didn't you? Don't do that. New standards and stuff...")
        elif confirmation == 'N' or confirmation == 'n':
            loop = False
        else:
            print("Invalid answer.")

looping = True
print("---- Bioinformatical (re)assignment application ----")
print("Connected to", BASEURI)
while looping:
    print("---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ")
    if sys.version_info[0] == 3:
        pid = input("Enter the PID of the project you'd wish to (re)assign or Q to quit: ")
    elif sys.version_info[0] == 2:
        pid = raw_input("Enter the PID of the project you'd wish to (re)assign or Q to quit: ")
    if pid != 'q' and pid != 'Q':
        namesetter(pid)
    else:
        looping = False
