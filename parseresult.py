#!/usr/bin/env python
import random
import math
from optparse import OptionParser
import numpy as np

def parseLogfile(logfile, strategy, outfile):
    wlf = open(logfile, "r")
    out = open(outfile, "a")
    result = {}
    for line in wlf:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(",")
        time = float(parts[0])
        identity = parts[1]
        action = parts[2]
        token = identity.split(" ")
        if(token[0]=="Job"):
            jid = token[1]
            if not result.has_key(jid):
                result[jid]={}
                result[jid]["start"] = []
                result[jid]["stop"] = []
            if(action=="STOP"):
                result[jid]["start"].append(time)
            if(action=="RESTART"):
                result[jid]["stop"].append(time)

    for jid in result.keys():
        if not result[jid]["start"]:
            print "no result"
            out.write("%s,%s,%f\n"%(jid,strategy,0.0))
        else:
            arr1 = np.array(result[jid]["start"])
            arr2 = np.array(result[jid]["stop"])
            stop_time = arr2 - arr1
            total=0
            for i in range(len(stop_time)):
                total += stop_time[i]
            line = "%s,%s,%f\n"%(jid,strategy,total)
            out.write(line)
    out.close()
    wlf.close()

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-f", "--file", dest = "file", type = "string", 
                    help = "name of log files") 
    p.add_option("-s", "--strategy", dest = "strategy", type = "string", 
                    help = "job placement strategy") 
    (opts, args) = p.parse_args()
    
    if not opts.file:
        print "please specify config file (-f)"
        p.print_help()
        exit()

    if not opts.strategy:
        print "please specify job placement strategy (-s)"
        p.print_help()
        exit()

    outfile = opts.file[0:opts.file.rfind(".")]+".csv"
    parseLogfile(opts.file, opts.strategy, outfile)
    exit()