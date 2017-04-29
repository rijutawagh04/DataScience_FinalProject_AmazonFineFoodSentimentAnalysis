install.packages("sentimentr")
library(sentimentr)
library(stringr)
library(plyr)
library(dplyr)
library(ggplot2)
library(tm)
library(wordcloud)
library(RColorBrewer)
setwd("C:/Users/Admin/Documents/DataScience/FinalProject/data")
amr  <- read.csv("Reviews.csv",stringsAsFactors = FALSE)
amr1 <- head(amr, 5000)
#text_vector<- as.character(levels(text)[as.character(text)])
sentrv <- with(amr1, sentiment_by(text))
qplot(amr1$Score, geom="histogram", binwidth = 1, main = "Histogram for Score", xlab = "Score", fill=I("blue"), col=I("red"), alpha=I(.8))
qplot(sentrv$ave_sentiment, geom="histogram", binwidth = 1, main = "Histogram for Avg Sentiment", xlab = "Avg Sentiment", fill=I("blue"), col=I("red"), alpha=I(.8))
summary(amr1$Score)
summary(sentrv$ave_sentiment)
best <- slice(amr1, top_n(sentrv, 5, ave_sentiment)$element_id)
bestt <- best[!duplicated(best$text),]
bestt <- data.matrix(bestt$text)
colnames(bestt) <- c("Top best reviews by sentiment score")
bestt

worst <- slice(amr1, top_n(sentrv, 5, -ave_sentiment)$element_id)
worstt <- worst[!duplicated(worst$text),]
worstt <- data.matrix(worst$text)
colnames(worstt) <- c("Top worst reviews by sentiment score")
worstt
