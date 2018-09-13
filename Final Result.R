setwd("/Users/Mine/Library/Mobile Documents/com~apple~CloudDocs/Documents/DSO 562 Fraud Analytics/Project 2")
dataa = read.csv("GBDT_oot_sorted dataset.csv")

df<-data.frame(Total_Records = 1,
               Num_Good = 1,
               Num_Bad = 1,
               Percent_Good = 1,
               Percent_Bad = 1,
               Cumulative_Good = 1,
               Cumulative_Bad = 1,
               Cumulative_Percent_Good = 1,
               Cumulative_Percent_Bad = 1,
               KS = 1,
               False_Positive_Ratio = 1
               )

cmlGood = 0
cmlBad = 0
totGood = sum(dataa$fraud == 0)
totBad = sum(dataa$fraud)

for (i in c(1:100)) {
  start = round(((i-1)/100)*17016)+1
  end = round((i/100)*17016)
  binwidth = end-start+1
  numGood = sum(dataa$fraud[start:end]==0)
  numBad = sum(dataa$fraud[start:end])
  pctGood = (numGood/binwidth)*100
  pctBad = (numBad/binwidth)*100
  #————————————————————cumulative statistics————————————————————#
  cmlGood = cmlGood + numGood
  cmlBad = cmlBad + numBad
  cmlPctGood = (cmlGood/totGood)*100
  cmlPctBad = (cmlBad/totBad)*100
  ks = cmlPctBad-cmlPctGood
  fpratio = cmlGood/cmlBad
  #———————————————————append into dataframe————————————————————#
  row = c(binwidth,numGood,numBad,pctGood,pctBad,cmlGood,cmlBad,cmlPctGood,cmlPctBad,ks,fpratio)
  df = rbind(df,row)
}  

result = df[2:21,]
write.csv(result,file = "final_result.csv")

bin = seq(1,20)
data = cbind(bin,result)

# ——————————————————— plot ———————————————————#

sp = spline(data$bin,data$Cumulative_Percent_Bad,n = 1000) # n表示平滑程度
plot(data$bin,data$Cumulative_Percent_Bad,
     col = "orange",
     pch = 19,
     xlim = c(0,20),
     ylim = c(0,30),
     main = "Fraud Detective Rate on Out-of-Time Dataset",
     xlab = "% Population",
     ylab = "% Fraud Caught")+
  lines(sp,
       col = "orange",
       type = "l")

library(ggplot2)
ggplot(dataa, aes(x = testOOT,log = "count"))+
  geom_histogram(bins = 100,
                 color = "white",
                 fill = "orange")+
  scale_y_log10()+
  xlab("Fraud Score")+
  ylab("Log of Frequency")+
  ggtitle("Fraud Score Distribution")+
  theme(plot.title = element_text(hjust = 0.5))
  