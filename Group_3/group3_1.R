#################################################################
##
## File:   group3_1.R
## Author: Stephen Dimig
## Description: This file contains the R commands used to solve the 
## first problem of group 3 for the cloud computing capstone class.
##
## Does the popularity distribution of airports follow a Zipf distribution? 
## If not, what distribution does it follow?
##
## First, the TopAirports.java program was used and all of the airports were
## output rather than just the top 10 (351 airports total). The results
## from HDFS were stored in the top.txt file. This file was read into R
## Studio. Then the zipfR package was used to show that the distribution was
## not quite a zipf distribution. The fitdistrplus R package was used
## to show that the distribution closely matched weibull distribution.
##
#################################################################
library(zipfR)
library(fitdistrplus)

# Read data and tidy up.
df <- read.table("top.txt", stringsAsFactors=FALSE)
names(df) <- c("origin", "flights")
mydf <- data.frame(1:351, df$flights)
names(mydf) <- c("rank", "flights")
mydf <- mydf[1:100, ]

# Use the zipfR package to show it is not a zipf distribution.
mydf.spc <- spc(Vm=mydf$flights, m=mydf$rank)
zm <- lnre("zm",mydf.spc)
zm.spc <- lnre.spc(zm,N(mydf.spc))
plot(mydf.spc, zm.spc)

# Use the fitdistrplus to show it is a weibull distribution.
f1 <- fitdist(x, "weibull")
f1
plotdist(df$flights,"weibull",para=list(shape=f1$estimate[1],scale=f1$estimate[2]))
