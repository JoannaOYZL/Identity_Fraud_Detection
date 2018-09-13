## KS Test
library(dplyr)
require(graphics)
setwd("~/Documents/fraud/project 2")

########### Read and Split the Data ####################
total=read.csv('new_vars.csv')
selected_variables = read.csv("selected_vars_45.csv")
data1 = total %>%
  select(as.character(selected_variables$x))
total = data.frame(fraud=total$fraud,record=total$record,data1)
oot=total[total$record>=77851,]
finaldata=total[total$record<77851,]

############ Sampling ####################
set.seed(1)
train=sample(77848,62278)
training=finaldata[train,]
testing=finaldata[-train,]
data=training

############ Split by Fraud ####################
data_1=data%>%
  filter(fraud==1)

data_0=data%>%
  filter(fraud==0)


############ Test Demo ####################
a_1=data_1%>%
  group_by(same_ssn_diff_address_28)%>%
  summarise(count1=n())

a_0=data_0%>%
  group_by(same_ssn_diff_address_28)%>%
  summarise(count0=n())

a=merge(a_1,a_0,all = TRUE)
a[is.na(a)]=0

test=ks.test(data_0[,3],data_1[,3])$statistic
#The warning message is due to the implementation of the KS test in R, 
#which expects a continuous distribution and thus there should not be any identical values in the two datasets i.e. ties.
#for this issue, should use fitter() to apply a small penalty

#test1=ks.test(jitter(a$count1),jitter(a$count0))$p.value
#plot(a$same_ssn_diff_address_3,a$count1,type="l",col="red")
#lines(a$same_ssn_diff_address_3,a$count0,col="green")

#d=density(a$count1[-1])
#m=density(a$count0[-1])

#plot(d)
#lines(m)


#### Run the Loop for Result

# result=numeric()
# 
# for(i in 5:164){
#  a_1=data_1%>%
#    group_by(data_1[,i])%>%
#    summarise(count1=n())
#  a_0=data_0%>%
#    group_by(data_0[,i])%>%
#    summarise(count0=n())
#  colnames(a_0)[1]='var'
#  colnames(a_1)[1]='var'
#  a=merge(a_1,a_0,all = TRUE)
#  a[is.na(a)]=0
#  set.seed(1) # for fix jitter() penalty
#  test=(ks.test(jitter(a$count1),jitter(a$count0))$statistic)*10
#  result=c(result,test)
# }

result=numeric()

for(i in 3:47){
  test=(ks.test(data_0[,i],data_1[,i])$statistic)*100
  result=c(result,test)
}


names=colnames(data[3:47]) #extract all variables 

df = data.frame(names,result) #combine results and variables

top30_vars=df%>%
  arrange(-result)%>%
  slice(1:30)            #Sort out top 30 variables      

vars<-as.character(top50_vars$names)
vars<-vars[-c(1,2,3,4,5)]
write.csv(vars,"selected_vars_45.csv")


vars=read.csv("selected_vars_45.csv")

training_data=data%>%
  select(2,vars)
testing_data=testing%>%
  select(vars)
training_data$fraud=as.factor(training_data$fraud)

write.csv(training_data,'training_data.csv')