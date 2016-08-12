#!/usr/bin/env python
import random
import math
from optparse import OptionParser
# import matplotlib.pyplot as plt
import numpy as np

X_MAX = 7200000

def parseLogfile(testname, clusterlog,failurelog, conf):
    out = open(testname+"_machine.csv", "w")
    outputline=""


    #read cluster log
    result={}
    clog = open(clusterlog, "r")
    for line in clog:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        cid = parts[0]
        zid = parts[1]
        pids = parts[2].split(",")
        s_pid = int(pids[0])
        e_pid = int(pids[1])
        pid_count = e_pid - s_pid + 1
        for i in range(pid_count):
            pid = str(s_pid+i)
            result[pid]={}
            result[pid]["LMZ"]=zid
            result[pid]["cid"]=cid
            result[pid]["time"]=[]
            result[pid]["status"]=[]
    clog.close()

    #find max sim time
    config = open(conf, "r")
    sim_time = 0
    for line in config:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split("=")
        if(parts[0]=="SIM_TIME_IN_DAYS"):
            sim_time = int(parts[1]) * 24 * 60 * 60
        if(parts[0]=="NUM_PART"):
            partitions = int(parts[1])
        if(parts[0]=="NUM_CLUSTER"):
            clusters = int(parts[1])
    config.close()
    numofpartitions = partitions * clusters

    #parse failure log
    flog = open(failurelog, "r")
    for line in flog:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        ftime = int(float(parts[0]))
        flagType = parts[1]
        ftype = parts[2]
        if ftype == "MT":
            zid = parts[4]
            if(flagType=="FS"):
                for pid in result.keys():
                    if(result[pid]["LMZ"]==zid):
                        result[pid]["time"].append(ftime)
                        result[pid]["status"].append(0)
            if(flagType=="FE"):
                for pid in result.keys():
                    if(result[pid]["LMZ"]==zid):
                        result[pid]["time"].append(ftime)
                        result[pid]["status"].append(1)
        elif ftype == "MNF":
            targetType = parts[3]
            if targetType == "Cluster":
                cid = parts[4]
                if(flagType=="FS"):
                    for pid in result.keys():
                        if(result[pid]["cid"]==cid):
                            result[pid]["time"].append(ftime)
                            result[pid]["status"].append(0)
                if(flagType=="FE"):
                    for pid in result.keys():
                        if(result[pid]["cid"]==cid):
                            result[pid]["time"].append(ftime)
                            result[pid]["status"].append(1)
            elif targetType == "Partition":
                thispid = parts[4]
                if(flagType=="FS"):
                    for pid in result.keys():
                        if(pid==thispid):
                            result[pid]["time"].append(ftime)
                            result[pid]["status"].append(0)
                if(flagType=="FE"):
                    for pid in result.keys():
                        if(pid==thispid):
                            result[pid]["time"].append(ftime)
                            result[pid]["status"].append(1)  
    flog.close()

    #calcuate down partitions
    down=[]
    for i in range(sim_time+1):
        count = 0
        for pid in result.keys():
            counter = 0
            if(counter<len(result[pid]["time"]) and i==result[pid]["time"][counter]):
                status = result[pid]["status"][counter]
                counter += 1
            else:
                if(counter>0):
                    status = result[pid]["status"][counter-1]
                else:
                    status = 1
            if(status==0):
                count += 1
        down.append(count)

    print float(sum(down))/len(down)
    # #calcuate down partitions
    # time=[]
    # down=[]
    # for i in range(sim_time+1):
    #     time.append(i)
    #     count = 0
    #     for pid in result.keys():
    #         if(result[pid]["status"]==0):
    #             count += 1
    #     down.append(count)

    # #calculate average down partitions
    # avg_down = float(sum(down))/len(down)
    # outputline += "avg_down=%f" % avg_down
    out.write(outputline)
    out.close()
    

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-t", "--testname", dest = "testname", type = "string", 
                    help = "name of test") 
    p.add_option("-l", "--clusterlog", dest = "clusterlog", type = "string", 
                    help = "path of cluster log file")
    p.add_option("-f", "--failurelog", dest = "failurelog", type = "string", 
                    help = "path of failure log file")
    p.add_option("-c", "--config", dest = "config", type = "string", 
                    help = "name of config log files")
    (opts, args) = p.parse_args()
    
    if not opts.testname:
        print "please specify test name (-t)"
        p.print_help()
        exit()

    if not opts.clusterlog:
        print "please specify path of cluster log file (-cl)"
        p.print_help()
        exit()

    if not opts.failurelog:
        print "please specify path of failure log file (-f)"
        p.print_help()
        exit()

    if not opts.config:
        print "please specify config file (-c)"
        p.print_help()
        exit()

    parseLogfile(opts.testname, opts.clusterlog, opts.failurelog, opts.config)
    exit()