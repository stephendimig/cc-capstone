#!/bin/bash

#################################################################
##
## File:   group2_1.sh
## Author: Stephen Dimig
## Description: This is a shell script to execute the commands to 
## answer the following question from Group 2 of the Cloud Computing
## capstone project.
##
## For each airport X, rank the top-10 airports in decreasing order 
## of on-time departure performance from X. See Task 1 Queries for 
## specific queries.
##
## The approach taken for the Group2 problems was to use pig to
## execute the query on the dataset rather than map reduce. Pig
## provides a higher level SQL-like lanuage that takes care of 
## all of the map reduce without requiring java code to do it.
##
## The results of the pig query were then extracted from HDFS
## and filtered through python to produce a file that included 
## all of the cql commands needed to load the data into Cassandra.
##
## There is supposed to be an interface for Pig and Cassandra but I 
## could not get it to work.
##
## The cql commands were then fed into cqlsh to load the data.
##
#################################################################

# Set up HDFS
hadoop fs -rm -r -f output
hadoop fs -mkdir output

# Run pig script to query dataset.
pig group2_2.pig

# Run python command to generate cql commands from pig output.
./group2_2.py

# Execute cql commands to load Cassandra database.
cqlsh < export.cql