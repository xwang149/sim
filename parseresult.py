#!/usr/bin/env python
import random
import math
from optparse import OptionParser
# import matplotlib.pyplot as plt
import numpy as np

X_MAX = 7200000

def testDowngrade(activetask, threshold):
    downgrade = -1
    for i in range(len(threshold)):
        if(threshold[i]==activetask):
            downgrade = 9-i
    return downgrade

def testUpgrade(activetask, threshold):
    upgrade = -1
    for i in range(len(threshold)):
        if(threshold[i]+1==activetask):
            upgrade = 9-i
    return upgrade

def parseLogfile(testname, joblogfile, conf, outfile):
    out = open(outfile+".csv", "w")
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
        numreplica = len(parts[2].split(","))
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

    max_diversity = numreplica

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
            break
    config.close()    

    # print thresholds
    outputline = ""
    for p in range(1, max_diversity+1):
        for z in range(p, max_diversity+1):
            singleresult = {}
            active = {}
            print "read %s_%s-%s" %(testname, p, z)
            wlf = open(testname+"_"+str(p)+"-"+str(z), "r")
            strategy = str(p)+"-"+str(z)
            downtimes = {}
            for line in wlf:
                line = line.strip('\n')
                line = line.strip('\r')
                if len(line)==0:
                    continue
                if "add" in line:
                    continue
                parts = line.split(",")
                time = int(parts[0])
                identity = parts[1]
                action = parts[2]
                token = identity.split(" ")
                if(token[0]=="Job" and action=="START"):
                    jid = token[1]
                    if not singleresult.has_key(jid):
                        singleresult[jid]={}
                        singleresult[jid]["activetasks"]=[]
                        singleresult[jid]["time"] = []
                        singleresult[jid]["activetasks"].append(numoftasks[jid])
                        singleresult[jid]["time"].append(time)
                        active[jid] = numoftasks[jid]
                        singleresult[jid]["downtime"]={}
                        downtimes[jid]={}
                        for grade in range(9,-1,-1):
                            downtimes[jid][grade]={}
                            downtimes[jid][grade]["start"]=[]
                            downtimes[jid][grade]["stop"]=[]
                if(token[0]=="Task"):
                    taskid = token[1]
                    jid = taskid.split("_")[0]
                    if(action=="STOP"):
                        active[jid] -= 1
                        singleresult[jid]["time"].append(time)
                        singleresult[jid]["activetasks"].append(active[jid])
                        downgrade = testDowngrade(active[jid], thresholds[jid])
                        # print "active %s: %s "%(jid, active[jid])
                        if(downgrade != -1):
                            downtimes[jid][downgrade]["start"].append(time)
                    if(action=="RESTART"):
                        active[jid] += 1
                        singleresult[jid]["time"].append(time)
                        singleresult[jid]["activetasks"].append(active[jid])
                        upgrade = testUpgrade(active[jid], thresholds[jid])
                        # print "active %s: %s "%(jid, active[jid])
                        if(upgrade != -1):
                            downtimes[jid][upgrade]["stop"].append(time)   
            # print downtimes
            #prepare data
            for jid in singleresult.keys():
                timeline = []
                taskline = []
                # maxline = []
                # counter = 0
                # for i in range(sim_time+1):
                #     timeline.append(i)
                #     maxline.append(numoftasks[jid])
                #     if(i==singleresult[jid]["time"][counter]):
                #         taskline.append(singleresult[jid]["activetasks"][counter])
                #         activenow = singleresult[jid]["activetasks"][counter]
                #         counter += 1
                #     else:
                #         taskline.append(activenow)
                #calculate percentage
                allactive = sim_time * numoftasks[jid]
                singleresult[jid]["time"].append(sim_time)
                actualactive = 0
                for i in range(len(singleresult[jid]["activetasks"])):
                    length = singleresult[jid]["time"][i+1] - singleresult[jid]["time"][i]
                    actualactive += singleresult[jid]["activetasks"][i] * length
                percentage = '{0:.2%}'.format(float(actualactive)/allactive)
                # print percentage
                # outputline += "%d,%d,%s,%s\n"%(p, z, jid, float(actualactive)/allactive)
                
                #plot figure
                # singleresult[jid]["activetasks"].append(singleresult[jid]["activetasks"][-1])
                # i=0
                # while(singleresult[jid]["time"][i]<= X_MAX):
                #     timeline.append(singleresult[jid]["time"][i]/3600.0)
                #     taskline.append(singleresult[jid]["activetasks"][i])
                #     i += 1
                # maxline = []      
                # for i in range(sim_time/60/60+1):
                #     timeline.append(i)
                #     maxline.append(numoftasks[jid])
                #     for j in range(len(singleresult[jid]["time"])):
                #         if(j==len(singleresult[jid]["time"])):
                #             taskline.append(singleresult[jid]["activetasks"][j])
                #         else:
                #             if(singleresult[jid]["time"][j]>i):
                #                 taskline.append(singleresult[jid]["activetasks"][j-1])
                #                 break
                
                # figname = outfile+"_"+jid+"_"+strategy+".svg"
                # fig, ax = plt.subplots()
                # plt.plot(timeline, taskline)
                # # plt.plot(timeline, maxline)
                # # plt.plot(singleresult[jid]["time"], singleresult[jid]["activetasks"], linewidth=0.5)
                # plt.axhline(y=numoftasks[jid], color='r')
                # # ax.fill_between(timeline, taskline, numoftasks[jid], facecolor='blue')
                # ymax = (numoftasks[jid]/10+1)*10
                # plt.ylim(0, ymax)
                # plt.yticks(np.arange(0, ymax, 10))
                # plt.xlim(0, X_MAX/3600.0)
                # # plt.xticks(np.arange(0, sim_time, sim_time/24/60/60/30))
                # plt.xlabel('time (hr)')
                # plt.ylabel('tasks')
                # plt.title("Job_"+jid+" with "+strategy+" strategy ( "+percentage+" )")
                # plt.legend(['active tasks', 'total tasks'], loc='lower right')
                # # plt.text(0.95,0.95, percentage,fontsize=12)
                # fig.savefig(figname, ext="svg")
                # plt.close()


            for jid in downtimes.keys():
                for grade in downtimes[jid].keys():
                    if not downtimes[jid][grade]["start"]:
                        singleresult[jid]["downtime"][grade] = 0
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
                        # print "J%s,Grade %d; total=%d" %(jid, grade, total)

            sorted_jids = singleresult.keys()
            sorted_jids = sorted(sorted_jids)
            for jid in sorted_jids:
                outputline += "%s,%s,%s,"%(p,z, jid)
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
    p.add_option("-c", "--config", dest = "config", type = "string", 
                    help = "name of config log files")
    (opts, args) = p.parse_args()
    
    if not opts.testname:
        print "please specify test name (-t)"
        p.print_help()
        exit()

    if not opts.joblog:
        print "please specify path of job log file (-j)"
        p.print_help()
        exit()

    if not opts.config:
        print "please specify config file (-c)"
        p.print_help()
        exit()

    parseLogfile(opts.testname, opts.joblog, opts.config, opts.testname)
    exit()