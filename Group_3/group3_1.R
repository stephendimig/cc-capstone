library(zipfR)
library(fitdistrplus)

df <- read.table("top.txt", stringsAsFactors=FALSE)
names(df) <- c("origin", "flights")
mydf <- data.frame(1:351, df$flights)
names(mydf) <- c("rank", "flights")

mydf <- mydf[1:100, ]
mydf.spc <- spc(Vm=mydf$flights, m=mydf$rank)
zm <- lnre("zm",mydf.spc)
zm.spc <- lnre.spc(zm,N(mydf.spc))
plot(mydf.spc, zm.spc)
f1 <- fitdist(x, "weibull")
f1
plotdist(df$flights,"weibull",para=list(shape=f1$estimate[1],scale=f1$estimate[2]))
