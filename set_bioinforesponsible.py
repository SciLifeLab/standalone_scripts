# -*- coding: utf-8 -*-

""" Calls up the genologics LIMS directly in order to more quickly
    set a bioinformatics responsible. Script can easily be altered
    to be used to set other values."""
    
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD

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
        print "{} not available in LIMS.".format(PID)
        return None
    
    #Enter project summary process
    stepname=['Project Summary 1.3']
    process=lims.get_processes(type=stepname, projectname=limsproject)
    #Error handling
    if process == []:
        print "{} for {} is not available in LIMS.".format(stepname, limsproject)
        return None

    loop = True
    while loop:
        if "Bioinfo responsible" in process[0].udf:
            response = process[0].udf["Bioinfo responsible"]
        else:
            response = "Unassigned"
        print "Existing Bioinfo responsible for project {} aka {}: {}".format(limsproject, PID, response.encode('utf-8'))
        newname = raw_input("Enter name of new Bioinfo responsible, or C to cancel: ")
        if newname != 'c' and newname != 'C':
            confirmation = raw_input("Project {} aka {} will have {} as new Bioinfo responsible, is this correct (Y/N)? ".format(limsproject, PID, newname))
            if confirmation == 'Y' or confirmation == 'y':
                try:
                    process[0].udf["Bioinfo responsible"] = newname
                    process[0].put()
                    print "Project {} aka {} assigned to {}".format(limsproject, PID, newname)
                    return None
                except UnicodeDecodeError:
                    #Weird solution due to put function
                    process[0].udf["Bioinfo responsible"] = response
                    print "You tried to use a special character didn't you. Don't do that. New standards and stuff..."
            elif confirmation == 'N' or confirmation == 'n':
                loop = False
            else:
                print "Invalid answer."
        else:
            return None

looping = True
print "---- Bioinformatical (re)assignment application ----"
print "Connected to", BASEURI
while looping:
    print ("---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ")
    pid = raw_input("Enter the PID of the project you'd wish to (re)assign or Q to quit: ") 
    if pid != 'q' and pid != 'Q':
        namesetter(pid)
    else:
        looping = False
