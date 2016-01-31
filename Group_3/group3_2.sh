#!/bin/bash

#################################################################
##
## File:   group3_2.sh
## Author: Stephen Dimig
## Description: This is a shell script to execute the commands to 
## answer the following question from Group 3 of the Cloud Computing
## capstone project.
##
## Tom wants to travel from airport X to airport Z. However, Tom also 
## wants to stop at airport Y for some sightseeing on the way. More 
## concretely, Tom has the following requirements (see Task 1 Queries 
## for specific queries):
## a. The second leg of the journey (flight Y-Z) must depart two days 
## after the first leg (flight X-Y). For example, if X-Y departs January 
## 5, 2008, Y-Z must depart January 7, 2008.
## b. Tom wants his flights scheduled to depart airport X before 12:00 
## PM local time and to depart airport Y after 12:00 PM local time.
## c. Tom wants to arrive at each destination with as little delay as 
## possible (Clarification 1/24/16: assume you know the actual delay of 
## each flight).
##
## The approach taken for the Group3 problems was to use pig to
## execute the query on the dataset rather than map reduce. Pig
## provides a higher level SQL-like lanuage that takes care of 
## all of the map reduce without ruquiring java code to do it.
##
## The results of the pig query were then extracted from HDFS
## and filtered through R to do final processing. R is much easier
## to use than pig, but does not scale and would not work with the
## larger dataset.
##
#################################################################

# Process input parameters
X="$1"
Y="$2"
Z="$3"
DATE="$4"

# Set up HDFS
hadoop fs -rm -r -f output
hadoop fs -mkdir output

# Run the pig script and store the result in a temporary file.
pig -param X=$X -param Y=$Y -param Z=$Z group3_2.pig
hadoop fs -cat output/pig/* > /tmp/data.txt

# Run the final R script.
Rscript group3_2.R $X $Y $Z $DATE
