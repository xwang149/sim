#!/usr/bin/env python
import random
import math
from optparse import OptionParser
import numpy as np

def testDowngrade(jobprofile, threshold):
    downgrade = -1
    for i in range(len(threshold)):
        if(jobprofile["activetasks"]==threshold[i]):
            downgrade = 9-i
    return downgrade

def testUpgrade(jobprofile, threshold):
    upgrade = -1
    for i in range(len(threshold)):
        if(jobprofile["activetasks"]==threshold[i]+1):
            upgrade = 9-i
    return upgrade

def parseLogfile(testname, joblogfile, outfile):
    out = open(outfile, "w")
    numoftasks = {}
    #find thresholds for each job
    thresholds = {}
    joblog = open(joblogfile, "r")
    for line in joblog:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        time = int(parts[0])
        jid = parts[1]
        if not thresholds.has_key(jid):
            thresholds[jid]=[]
        numTasks = parts[3].split(",")
        totalTasks = 0
        for i in range(len(numTasks)):
            totalTasks += int(numTasks[i])
        numoftasks[jid]=totalTasks
        for i in range(9,-1,-1):
            thresholds[jid].append(int(round(totalTasks*i/10.0)))
    joblog.close()
    # print thresholds
    downtimes = {}
    finalresult = {}
    singleresult = {}
    outputline = ""
    for p in range(1, 4):
        for z in range(1, 4):
            print "read %s_%s-%s" %(testname, p, z)
            wlf = open(testname+"_"+str(p)+"-"+str(z), "r")
            strategy = str(p)+"-"+str(z)
            downtimeresult = {}
            for line in wlf:
                line = line.strip('\n')
                line = line.strip('\r')
                if len(line)==0:
                    continue
                if "add" in line:
                    continue
                parts = line.split(",")
                time = float(parts[0])
                identity = parts[1]
                action = parts[2]
                token = identity.split(" ")
                if(token[0]=="Job" and action=="START"):
                    jid = token[1]
                    if not singleresult.has_key(jid):
                        singleresult[jid]={}
                        singleresult[jid]["activetasks"] = numoftasks[jid]
                        singleresult[jid]["downtime"]={}
                        singleresult[jid]["downtasks"]=[]
                        downtimes[jid]={}
                        for grade in range(9,-1,-1):
                            downtimes[jid][grade]={}
                            downtimes[jid][grade]["start"]=[]
                            downtimes[jid][grade]["stop"]=[]
                if(token[0]=="Task"):
                    taskid = token[1]
                    jid = taskid.split("_")[0]
                    if(action=="STOP"):
                        singleresult[jid]["downtasks"].append(taskid)
                        singleresult[jid]["activetasks"] -= 1
                        downgrade = testDowngrade(singleresult[jid], thresholds[jid])
                        if(downgrade != -1):
                            downtimes[jid][downgrade]["start"].append(time)
                    if(action=="RESTART"):
                        singleresult[jid]["downtasks"].remove(taskid)
                        singleresult[jid]["activetasks"] += 1
                        upgrade = testUpgrade(singleresult[jid], thresholds[jid])
                        if(upgrade != -1):
                            downtimes[jid][upgrade]["stop"].append(time)  

            for jid in downtimes.keys():
                for grade in downtimes[jid].keys():
                    if not downtimes[jid][grade]["start"]:
                        print "no result"
                    else:
                        arr1 = np.array(downtimes[jid][grade]["start"])
                        arr2 = np.array(downtimes[jid][grade]["stop"])
                        # stop_time = arr2 - arr1
                        if(len(arr2)!=len(arr1)):
                            print "not equal: jid=%s grade=%s" %(jid, grade)
                        else:
                            stop_time = arr2 - arr1
                        total=0
                        for i in range(len(stop_time)):
                            total += stop_time[i]
                        singleresult[jid]["downtime"][grade]=total

            sorted_jids = singleresult.keys()
            sorted_jids = sorted(sorted_jids)
            for jid in sorted_jids:
                outputline += "%s,%s,"%(strategy, jid)
                sorted_keys = singleresult[jid]["downtime"].keys()
                sorted_keys = sorted(sorted_keys, reverse=True)
                for grade in sorted_keys:
                    outputline += "%s," % singleresult[jid]["downtime"][grade]
                outputline += "\n"
            wlf.close()
    out.write(outputline)
    out.close()
    

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-t", "--testname", dest = "testname", type = "string", 
                    help = "name of test") 
    p.add_option("-j", "--joblog", dest = "joblog", type = "string", 
                    help = "path of job log file")
    (opts, args) = p.parse_args()
    
    if not opts.testname:
        print "please specify test name (-t)"
        p.print_help()
        exit()

    if not opts.joblog:
        print "please specify path of job log file (-j)"
        p.print_help()
        exit()

    outfile = opts.testname+".csv"
    parseLogfile(opts.testname, opts.joblog, outfile)
    exit()