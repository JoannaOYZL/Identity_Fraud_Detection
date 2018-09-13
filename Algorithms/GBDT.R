setwd("/Users/zimengcao/Documents/DSO 562 Fraud Analytics/Project 2")
training = read.csv("training.csv")
testing = read.csv("testing.csv")
oot = read.csv("oot.csv")

library(gbm)
library(dplyr)

# Training model:
training_model <- gbm(fraud~.-record,
                      distribution = 'bernoulli',
                      data=training,
                      n.trees=1000,
                      shrinkage = 0.01,
                      cv.folds=5)


best.iter <- gbm.perf(training_model)
#best.iter
summary.gbm(training_model,best.iter)
#par(mfrow=c(1,3))
#plot.gbm(training_model,"last_address",best.iter)
#plot.gbm(training_model,"same_name_1",best.iter)
#plot.gbm(training_model,"last_ssn",best.iter)

#——————————————————————————————————————————————————————————#
# Test in training model:
testTraining <- predict(training_model,training,best.iter)
print(sum((training$fraud-testTraining)^2))
pfmTraining = data.frame(training,testTraining)
see1 = pfmTraining %>%
  select(fraud,testTraining) %>%
  arrange(-testTraining)
# 62,278 records, 10% cutoff is record 6228
# calculate FDR@10%, false positive ratio * rate:
FDR10Train = sum(see1$fraud[1:6228])/sum(see1$fraud)
fpRatioTrain = sum(see1$fraud[1:6228]==0)/sum(see1$fraud[1:6228])
fpRTrain = sum(see1$fraud[1:6228]==0)/6228

#——————————————————————————————————————————————————————————#
# Test in testing model:
testTesting <- predict(training_model,testing,best.iter)
print(sum((testing$fraud-testTesting)^2))
pfmTesting = data.frame(testing,testTesting)
see2 = pfmTesting %>%
  select(fraud,testTesting) %>%
  arrange(-testTesting)
# 15,570 records, 10% cutoff is record 1557
# calculate FDR@10%, false positive ratio * rate:
FDR10Test = sum(see2$fraud[1:1557])/sum(see2$fraud)
fpRatioTest = sum(see2$fraud[1:1557]==0)/sum(see2$fraud[1:1557])
fpRTest = sum(see2$fraud[1:1557]==0)/1557

#——————————————————————————————————————————————————————————#
# Test in OOT model:
## model:
colnames(training)
colnames(testing)
data1 = read.csv("training+testing.csv")
nrow(data1)
training_model2 <- gbm(fraud~.-record,
                      distribution = 'bernoulli',
                      data=data1,
                      n.trees=1000,
                      shrinkage = 0.01,
                      cv.folds=5)

best.iter2 <- gbm.perf(training_model2)
summary.gbm(training_model2,best.iter2)
par(mfrow=c(1,3))
plot.gbm(training_model2,"last_address",best.iter2)
plot.gbm(training_model2,"same_name_1",best.iter2)
plot.gbm(training_model2,"last_ssn",best.iter2)
## test
testOOT <- predict(training_model2,oot,best.iter2)
print(sum((oot$fraud-testOOT)^2))
pfmOOT = data.frame(oot,testOOT)
see3 = pfmOOT %>%
  select(fraud,testOOT) %>%
  arrange(-testOOT)
# 17,016 records, 10% cutoff is record 1702
# calculate FDR@10%, false positive ratio * rate:
sum(see3$fraud[1:1702])
FDR10_OOT = sum(see3$fraud[1:1702])/sum(see3$fraud)
fpRatio_OOT = sum(see3$fraud[1:1702]==0)/sum(see3$fraud[1:1702])
fpR_OOT = sum(see3$fraud[1:1702]==0)/1702

sum(oot$fraud)
write.csv(see3,file = "GBDT_oot_sorted dataset.csv")

#————————————————————Show the statistics————————————————————#

FDR10 = c(FDR10Train,FDR10Test,FDR10_OOT)
FalsePositiveRatio = c(fpRatioTrain,fpRatioTest,fpRatio_OOT)
FalsePositiveRate = c(fpRTrain,fpRTest,fpR_OOT)
performStat = data.frame(FDR10,FalsePositiveRatio,FalsePositiveRate)
row.names(performStat) = c("train","test","OOT")
