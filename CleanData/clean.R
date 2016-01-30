#################################################################
##
## File:   clean.R
## Author: Stephen Dimig
## Description:
## This is a small R script that cleans the he dataset used in the Capstone 
## project. The dataset  contains data and statistics from the US Department 
## of Transportation on Aviation, Maritime, Highway, Transit, Rail, Pipeline, 
## Bike/Pedestrian and other modes of transportation in CSV format. The 
## dataset we are using does not extend beyond 2008. In this Capstone, 
## we will concentrate exclusively on the Aviation portion of the dataset, 
## which contains flight data such as departure and arrival delays, flight 
## times, etc. 
##
## This script assumes you have created an EBS volume from the snap-e1608d88 
## snapshot and attached the volume to your development VM as described here:
## http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumes.html.
##
## I attached the volume to the /dev/xvdg device as and mounted it as follows:
## $ mkdir /ebsdata
## $ mount /dev/xvdg /ebsdata
##
## Since the script can take some time to run, I use nohup.
## $ nohup Rscript clean.R &
##
## The results of running this script will be a reduced dataset stored in
## hdfs in the airline_ontime directory that includes the following rows:
## 
## FlightDate, FlightNum, Origin, Dest, UniqueCarrier, Carrier, ArrTime, 
## ArrDelay, ArrDelayMinutes, DepTime, DepDelay, DepDelayMinutes, 
## and DayOfWeek.
##
## This script uses the dplyr and rhdfs packages.
##
#################################################################

library(rhdfs)
library(dplyr)
rootdirname <- "airline_ontime"
hdfs.init()
hdfs.mkdir(rootdirname)

# Iterate across all directories in EBS data.
dirs <- list.dirs("/ebsdata/aviation/airline_ontime/")
for(i in 2:length(dirs))
{
    files <- list.files(dirs[i])
    elems <- strsplit(dirs[i], "/")
    
    # Iterate across all files directory
    dirname <- rootdirname 
    for(j in 1:length(files))
    {
        elems <- strsplit(files[j], "/")
        zipfile <- sprintf("%s/%s", dirs[i], files[j])
        csvfile <- gsub(".zip", ".csv", elems[length(elems)])
        txtfile <- gsub(".zip", ".txt", elems[length(elems)])
        if(csvfile != "On_Time_On_Time_Performance_2008_11.csv" & 
               csvfile != "On_Time_On_Time_Performance_2008_12.csv")
        {
            # Just some progress related output statements that end up in
            # nohup.out.
            print(sprintf("dirname=%s", dirname))
            print(sprintf("zipfile=%s", zipfile))
            print(sprintf("csvfile=%s", csvfile))  
            print(sprintf("txtfile=%s", txtfile))
            
            # Unzip and read each file from the EBS volume
            df <- read.csv(unz(zipfile, csvfile), stringsAsFactors=FALSE)
            
            # Explicitly convert the date.
            df$FlightDate <- as.Date(df$FlightDate)
            
            # Select only certain rows required for the capstone.
            my_df <- select(df, FlightDate, FlightNum, Origin, Dest, UniqueCarrier, Carrier, ArrTime, ArrDelay, ArrDelayMinutes, DepTime, DepDelay, DepDelayMinutes, DayOfWeek)
            
            # Write cleaned file, put it in HDFS, and remove local copy.
            write.csv(my_df, file=txtfile, quote=FALSE, col.names=FALSE)
            filename <- sprintf("%s/%s", dirname, txtfile)
            hdfs.put(txtfile, dirname)
            file.remove(txtfile)
        }
    }
}
