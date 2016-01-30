library(rhdfs)
library(dplyr)

args <- commandArgs(trailingOnly = TRUE)

X <- args[1]
Y <- args[2]
Z <- args[3]

df <- read.table("/tmp/data.txt", stringsAsFactors=FALSE)
names(df) <- c("flightno", "origin", "dest", "carrier", "date", "dep_time", "delay")

df$date <- as.Date(df$date)
x2y <- df[df$origin == X, ]
y2z <- df[df$origin == Y, ]

for(i in 1:dim(x2y)[1])  # for each row
{
    d1 <- x2y[i, ]$date
    flights <- y2z[y2z$date - d1 == 2, ]
    flights <- flights[order(flights$delay), ]
    if(dim(flights)[1] > 0)
    {
        print(sprintf("Originating Flight[%d]", i))
        print("===================")
        print(x2y[i, ])
        
        print("Terminating Flight")
        print("===================")
        print(flights)
        print
    }
}