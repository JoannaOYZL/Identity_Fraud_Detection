app=read.csv("applications.csv")
app=app %>%
  arrange(date)
colnames(app)[1]='record'
library(dplyr)
library(lubridate)

# feature selection: stepwise logistic regression
final=read.csv('new_vars.csv')
coln=read.csv('top30_vars.csv')
finalx=final %>%
  dplyr::select(as.character(coln$names),fraud)
holdout=finalx[77849:94864,]
finaldata=finalx[1:77848,]

# separating training, testing
set.seed(1)
train=sample(77848,62278)
training=finaldata[train,]
testing=finaldata[-train,]

## backwards(default) 
fullmod=glm(fraud~.,data=training,family=binomial)  # change effective variable
summary(fullmod)
nothing=glm(fraud~1,data=training,family = binomial)
summary(nothing)

redmod1=glm(fraud~ var)  # fill in variables that are significant
backwards=step(fullmod)  # backwards is default
formula(backwards)  # results for backwards selection
summary(backwards)  # model results for backwards selection

## forwards
forwards=step(nothing,scope=list(lower=formula(nothing),upper=formula(fullmod)),direction='fowards')
formula(forwards)

## bothway
bothways=step(nothing,list(lower=formula(nothing),upper=formula(fullmod)),direction='both',trace=0,silent=TRUE)
formula(bothways)
summary(bothways)

## prediction for training 
probs=predict(bothways,type='response')
trainresult=rep(0,62278)
trainresult[probs>0.5]=1
table(trainresult,training$fraud)

## prediction for testing
probstest=predict(bothways,data.frame(testing[,c('last_address','same_name_1','same_homephone_diff_address_1',
                                                 'last_ssn','same_homephone_3',
                                                 'same_homephone_diff_ssnname_3')]),type='response')
testresult=rep(0,15570)
testresult[probstest>0.5]=1
table(testresult,testing$fraud)

## prediction for holdout set
bothways2=glm(fraud~last_address + same_name_1 + same_homephone_diff_address_1 + 
                last_ssn + same_homephone_3 + same_homephone_diff_ssnname_3,finaldata,family=binomial)
probsholdout=predict(bothways2,data.frame(holdout[,c('last_address','same_name_1','same_homephone_diff_address_1',
                                                     'last_ssn','same_homephone_3',
                                                     'same_homephone_diff_ssnname_3')]),type='response')
holdoutresult=rep(0,17016)
holdoutresult[probsholdout>0.5]=1
table(holdoutresult,holdout$fraud)

## Fraud Detection Rate for train/test/holdout sets
trainpre_true=data.frame(training,probs)
trainpre_true=trainpre_true %>%
  mutate(pred_fraud=ifelse(probs>0.2798245,1,0))
quantile(probs,0.9)
nrow(trainpre_true[trainpre_true$pred_fraud==1 & trainpre_true$fraud==1,])/nrow(trainpre_true[trainpre_true$fraud==1,])

testpre_true=data.frame(testing,probstest)
testpre_true=testpre_true %>%
  mutate(pred_fraud=ifelse(probstest>0.2811018,1,0))
quantile(probstest,0.9)
nrow(testpre_true[testpre_true$pred_fraud==1 & testpre_true$fraud==1,])/nrow(testpre_true[testpre_true$fraud==1,])

holdoutpre_true=data.frame(holdout,probsholdout)
holdoutpre_true=holdoutpre_true %>%
  mutate(pred_fraud=ifelse(probsholdout>0.3243423,1,0))
quantile(probsholdout,0.9)
nrow(holdoutpre_true[holdoutpre_true$pred_fraud==1 & holdoutpre_true$fraud==1,])/nrow(holdoutpre_true[holdoutpre_true$fraud==1,])