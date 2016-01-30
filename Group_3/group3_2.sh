#!/bin/bash

X="$1"
Y="$2"
Z="$3"

hadoop fs -rm -r -f output
hadoop fs -mkdir output

pig -param X=$X -param Y=$Y -param Z=$Z group3_2.pig
hadoop fs -cat output/pig/* > /tmp/data.txt

Rscript group3_2.R $X $Y $Z
