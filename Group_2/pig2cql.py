#!/usr/bin/python

#################################################################
##
## File:   group2_1.py
## Author: Stephen Dimig
## Description: This is a python script that takes the results of
## the pig query, extracts the output from HDFS and filters it to 
## produce a file that includes all of the cql commands needed to 
## load the data into Cassandra.
##
## There is supposed to be an interface for Pig and Cassandra but I 
## could not get it to work.
##
#################################################################

import re
import io
import subprocess
proc = subprocess.Popen(['hadoop','fs', '-cat', '/user/root/output/pig/*'],stdout=subprocess.PIPE)

f = open('export.cql', 'w')
f.write("truncate mykeyspace.results1;\n")
for line in proc.stdout:
    match = re.search(r'(\S{3})\s*(\S{2}|\S{2} [(]{1}[0-9]+[)]{1})\s*([\d.]+)', line)
    f.write("INSERT INTO mykeyspace.results1 (origin, unique_carrier, dep_delay_avg) VALUES('" + match.group(1) +  "', '" + match.group(2) + "', " + match.group(3) + ");\n")
