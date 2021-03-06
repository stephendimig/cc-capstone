#################################################################
##
## File:   group3_1.R
## Author: Stephen Dimig
## Description: This file contains an R script used to solve the 
## second problem of group 3 for the cloud computing capstone class.
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
## First, a pig script is used to pull data that meets criteria from the
## large data set. The script does enough to prune the data so that R
## can procees it efficiently. Next this R script is invoked on the 
## results to do the final processing.
##
#################################################################

library(rhdfs)
library(dplyr)

# Command line processing
args <- commandArgs(trailingOnly = TRUE)
X <- args[1]
Y <- args[2]
Z <- args[3]
DATE <- as.Date(args[4], "%m/%d/%Y")

# Read data 
df <- read.table("/tmp/data.txt", stringsAsFactors=FALSE)
names(df) <- c("flightno", "origin", "dest", "carrier", "date", "dep_time", "delay")

# Clean data
df$date <- as.Date(df$date)
x2y <- df[df$origin == X & df$dest == Y & df$date == DATE, ]
y2z <- df[df$origin == Y & df$dest == Z, ]

if(dim(x2y)[1] > 0)
{
    x2y <- x2y[order(x2y$delay), ]
    # Process data
    print(sprintf("%s -> %s Flights", X, Y))
    print("===================")
    print(x2y)
    print("")
    
    d1 <- DATE
    flights <- y2z[y2z$date - d1 == 2, ]
    flights <- flights[order(flights$delay), ]
    
    # Output results
    if(dim(flights)[1] > 0)
    {
        print(sprintf("%s -> %s Flights", Y, Z))
        print("===================")
        print(flights)
        print("")
    }
    
} else {
    print(sprintf("No flights found matching criteria X=%s; Y=%s; Z=%s; DATE=%s", X, Y, Z, DATE))
}