
# Loading the packages and data
library(dplyr)
library(stringr)
library(lubridate)
library(tidyr)
library(scales)
library(DBI)
library(RSQLite)
data=read.csv("applications.csv")
summary(data)
head(data,30)

# Data preprocessing

#---------------------- date column ------------------------
## Convert the date into time format
data$date=mdy(data$date)
data$date=as.character(data$date)

#---------------------- dob column ------------------------
### transform year format
data$dob=as.character(data$dob)

### Split the dob into month, day, and year with only the last two digits
dob_split=data %>%
  separate(dob,into=c('month','day','year_l2'),sep='/') %>%
  select(month,day,year_l2,record)
dob_split$year=as.numeric(dob_split$year_l2)

### add 0 to the years with only one digit. so '7' is tramsformed into '07'
dob_split$year=str_pad(dob_split$year_l2, 2, pad='0')
### set all the date to 1900s.
dob_split=mutate(dob_split, year_f2=19)
str(dob_split)

### Combine all the the year_f2 with year_l2 to get a four digit year column, and then combine all the year, month, and day back to get a transformed dob.
dob_split$year_l2=as.character(dob_split$year_l2)
dob_agg=dob_split %>%
  unite(year, year_f2, year_l2,sep='', remove=FALSE) %>%
  unite(dob_n, month, day, year, sep='/', remove=FALSE)

### merge new dob, year, month with original data dataset 
data=merge(data, dob_agg, by='record')
data=data %>%
  select(-year_f2, -year_l2, -day, -month, -year.1, -year)

### change the dob column into time format with lubridate
data$dob=as.Date(data$dob_n, "%m/%d/%Y")
data=data[, -11]#delete dob_n
data$dob=as.character(data$dob)

# Variable Creation with RSQLite
# SQL proves to be able to construct a whole group of vairables under the same logic much faster than R in this project
app = dbConnect(RSQLite::SQLite(), "application database") #create a database
dbWriteTable(app, "data", data) #write a table into the database
dbListTables(app) #list all the tables in the db
dbListFields(app, "data") #List all the variable names in the table "data"
dbReadTable(app, "data") #read the content in the table "data"

## n_ssnPerAdd
dbGetQuery(app, "
           SELECT address, COUNT(DISTINCT ssn) as n_ssnPerAdd
           FROM data
           GROUP BY address
           ORDER BY n_ssnPerAdd DESC")

## n_ssnPerPho
dbGetQuery(app, "
           SELECT homephone, COUNT(DISTINCT ssn) as n_ssnPerPho
           FROM data
           GROUP BY homephone
           ORDER BY n_ssnPerPho DESC")

## n_zipPerSSN
dbGetQuery(app, "
           SELECT SSN, COUNT(DISTINCT zip5) as n_zipPerSSN
           FROM data
           GROUP BY SSN
           ORDER BY n_zipPerSSN DESC")

## n_zipPerName
dbGetQuery(app, "
           SELECT firstname, lastname, COUNT(DISTINCT zip5) as n_zipPerName
           FROM data
           GROUP BY firstname, lastname
           ORDER BY n_zipPerName DESC")

## n_zipPerNameDob
dbGetQuery(app, "
           SELECT firstname, lastname, dob, COUNT(DISTINCT zip5) as n_zipPerNameDob
           FROM data
           GROUP BY firstname, lastname, dob
           ORDER BY n_zipPerNameDob DESC")

## n_ssnPerNameDob
dbGetQuery(app, "
           SELECT firstname, lastname, dob, COUNT(DISTINCT ssn) as n_ssnPerNameDob
           FROM data
           GROUP BY firstname, lastname, dob
           ORDER BY n_ssnPerNameDob DESC")


# Aggregating all the above variables into the original dataset
data_n=dbGetQuery(app,"
                  SELECT * FROM data 
                  LEFT JOIN (SELECT address, COUNT(DISTINCT ssn) as n_ssnPerAdd FROM data GROUP BY address) t1
                  USING (address)
                  LEFT JOIN (SELECT homephone, COUNT(DISTINCT ssn) as n_ssnPerPho FROM data GROUP BY homephone) t2
                  USING (homephone)
                  LEFT JOIN (SELECT ssn, COUNT(DISTINCT zip5) as n_zipPerSSN FROM data GROUP BY ssn) t3
                  USING (ssn)
                  LEFT JOIN (SELECT firstname, lastname, COUNT(DISTINCT zip5) as n_zipPerName
                  FROM data
                  GROUP BY firstname, lastname) t4
                  USING (firstname, lastname)
                  LEFT JOIN (SELECT firstname, lastname, dob, COUNT(DISTINCT zip5) as n_zipPerNameDob
                  FROM data
                  GROUP BY firstname, lastname, dob) t5
                  USING (firstname, lastname, dob)
                  LEFT JOIN (SELECT firstname, lastname, dob, COUNT(DISTINCT ssn) as n_ssnPerNameDob
                  FROM data
                  GROUP BY firstname, lastname, dob) t6
                  USING (firstname, lastname, dob)")

## same_ssn: the number of same ssn as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_ssn=dbGetQuery(app, "
                    SELECT a.record,
                    COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_ssn_1,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_ssn_3,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_ssn_7,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_ssn_14,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_ssn_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_ssn_diff_address_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_ssn_diff_phone_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_ssn_diff_bdname_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zipname_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zipname_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zipname_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zipname_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) -1 AS same_ssn_diff_zipname_30
                    FROM data a, data b
                    WHERE a.ssn = b.ssn and a.record >= b.record
                    GROUP BY 1
                    ")

## same_fullname: the number of same applicant name as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_fullname=dbGetQuery(app, "
                         SELECT a.record,
                         COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_name_1,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_name_3,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_name_7,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_name_14,
                         COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_name_30,
                         
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_name_diff_address_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_name_diff_address_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_name_diff_address_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_name_diff_address_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_name_diff_address_30,
                         
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_name_diff_zip_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS same_name_diff_zip_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS same_name_diff_zip_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS same_name_diff_zip_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS same_name_diff_zip_30,
                         
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.dob ELSE NULL END) -1 AS same_name_diff_bd_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.dob ELSE NULL END) -1 AS same_name_diff_bd_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.dob ELSE NULL END) -1 AS same_name_diff_bd_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.dob ELSE NULL END) -1 AS same_name_diff_bd_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.dob ELSE NULL END) -1 AS same_name_diff_bd_30,
                         
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.ssn ELSE NULL END) -1 AS same_name_diff_ssn_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS same_name_diff_ssn_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS same_name_diff_ssn_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS same_name_diff_ssn_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS same_name_diff_ssn_30,
                         
                         COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_name_diff_phone_1, 
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_name_diff_phone_3,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_name_diff_phone_7,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_name_diff_phone_14,
                         COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_name_diff_phone_30
                         FROM data a, data b
                         WHERE a.firstname = b.firstname AND a.lastname = b.lastname
                         AND a.record >= b.record
                         GROUP BY 1")

## same_dbname: the number of same birthday and name combination as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_dbname=dbGetQuery(app, "
                       SELECT a.record,
                       COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_dbname_1,
                       COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_dbname_3,
                       COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_dbname_7,
                       COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_dbname_14,
                       COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_dbname_30,
                       
                       COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_dbname_diff_address_1, 
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_dbname_diff_address_3,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_dbname_diff_address_7,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_dbname_diff_address_14,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_dbname_diff_address_30,
                       
                       COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_dbname_diff_zip_1, 
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS same_dbname_diff_zip_3,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS same_dbname_diff_zip_7,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS same_dbname_diff_zip_14,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS same_dbname_diff_zip_30,
                       
                       COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.ssn ELSE NULL END) -1 AS same_dbname_diff_ssn_1, 
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.ssn ELSE NULL END) -1 AS same_dbname_diff_ssn_3,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.ssn ELSE NULL END) -1 AS same_dbname_diff_ssn_7,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.ssn ELSE NULL END) -1 AS same_dbname_diff_ssn_14,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.ssn ELSE NULL END) -1 AS same_dbname_diff_ssn_30,
                       
                       COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_dbname_diff_phone_1, 
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_dbname_diff_phone_3,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_dbname_diff_phone_7,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_dbname_diff_phone_14,
                       COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_dbname_diff_phone_30
                       FROM data a, data b
                       WHERE a.firstname = b.firstname AND a.lastname = b.lastname AND a.dob = b.dob
                       AND a.record >= b.record
                       GROUP BY 1")


## same_phone: the number of same phone number as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_phone=dbGetQuery(app, "
                      SELECT a.record,
                      COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_homephone_1,
                      COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_homephone_3,
                      COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_homephone_7,
                      COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_homephone_14,
                      COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_homephone_30,
                      
                      COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_1, 
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_homephone_diff_address_30,
                      
                      COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip_1, 
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip_3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip_7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip_14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS same_homephone_diff_zip_30,
                      
                      COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_1, 
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_homephone_diff_bdname_30,
                      
                      COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_homephone_diff_ssnname_1, 
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_homephone_diff_ssnname_3,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_homephone_diff_ssnname_7,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_homephone_diff_ssnname_14,
                      COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_homephone_diff_ssnname_30
                      FROM data a, data b
                      WHERE a.homephone = b.homephone 
                      AND a.record >= b.record
                      GROUP BY 1
                      ")


## same_zip: the number of same zip code as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_zip=dbGetQuery(app, "
                    SELECT a.record,
                    COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_zip_1,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_zip_3,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_zip_7,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_zip_14,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_zip_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.address ELSE NULL END) -1 AS same_zip_diff_address_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.address ELSE NULL END) -1 AS same_zip_diff_address_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.address ELSE NULL END) -1 AS same_zip_diff_address_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.address ELSE NULL END) -1 AS same_zip_diff_address_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.address ELSE NULL END) -1 AS same_zip_diff_address_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_zip_diff_phone_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_zip_diff_phone_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_zip_diff_phone_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_zip_diff_phone_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_zip_diff_phone_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip_diff_bdname_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip_diff_bdname_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip_diff_bdname_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip_diff_bdname_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_zip_diff_bdname_30,
                    
                    COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip_diff_ssnname_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip_diff_ssnname_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip_diff_ssnname_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip_diff_ssnname_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_zip_diff_ssnname_30
                    FROM data a, data b
                    WHERE a.zip5 = b.zip5
                    AND a.record>=b.record
                    GROUP BY 1
                    ")

## same_address: the number of same address as the current record appeared within different time period (1, 3, 7, 14, 30 days).
same_address=dbGetQuery(app, "
                        SELECT a.record,
                        COUNT(CASE WHEN a.date = b.date THEN a.record ELSE NULL END) -1 AS same_address_1,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.record ELSE NULL END) -1 AS same_address_3,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.record ELSE NULL END) -1 AS same_address_7,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.record ELSE NULL END) -1 AS same_address_14,
                        COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.record ELSE NULL END) -1 AS same_address_30,
                        
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.zip5 ELSE NULL END) -1 AS same_address_diff_zip_30,
                        
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.homephone ELSE NULL END) -1 AS same_address_diff_phone_30,
                        
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.dob || b.lastname ELSE NULL END) -1 AS same_address_diff_bdname_30,
                        
                        COUNT(DISTINCT CASE WHEN a.date = b.date THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_address_diff_ssnname_1, 
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_address_diff_ssnname_3,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_address_diff_ssnname_7,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_address_diff_ssnname_14,
                        COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN b.firstname || b.ssn || b.lastname ELSE NULL END) -1 AS same_address_diff_ssnname_30
                        FROM data a, data b
                        WHERE a.address = b.address
                        AND a.record>=b.record
                        GROUP BY 1
                        ")

## number of fraud under each unique ssn
fraud_per_ssn=dbGetQuery(app, "
                         SELECT a.record,
                         SUM(CASE WHEN a.date = b.date THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_ssn_1,
                         SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_ssn_3,
                         SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_ssn_7,
                         SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_ssn_14,
                         SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_ssn_30
                         FROM data a, data b
                         WHERE a.ssn = b.ssn
                         AND a.record>=b.record
                         GROUP BY 1
                         ")

## number of fraud under each unique homephone
fraud_per_phone=dbGetQuery(app, "
                           SELECT a.record,
                           SUM(CASE WHEN a.date = b.date THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_phone_1,
                           SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_phone_3,
                           SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_phone_7,
                           SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_phone_14,
                           SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_phone_30
                           FROM data a, data b
                           WHERE a.homephone = b.homephone
                           AND a.record>=b.record
                           GROUP BY 1
                           ")

## number of fraud under each unique dbname
fraud_per_dbname=dbGetQuery(app, "
                            SELECT a.record,
                            SUM(CASE WHEN a.date = b.date THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_dbname_1,
                            SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_dbname_3,
                            SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_dbname_7,
                            SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_dbname_14,
                            SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_dbname_30
                            FROM data a, data b
                            WHERE a.firstname = b.firstname and a.lastname = b.lastname and a.dob = b.dob
                            AND a.record>=b.record
                            GROUP BY 1
                            ")

## number of fraud under each unique address
fraud_per_address=dbGetQuery(app, "
                             SELECT a.record,
                             SUM(CASE WHEN a.date = b.date THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_address_1,
                             SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_address_3,
                             SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_address_7,
                             SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_address_14,
                             SUM(CASE WHEN a.date - b.date BETWEEN 0 AND 29 THEN a.fraud ELSE 0 END) - a.fraud AS fraud_per_address_30
                             FROM data a, data b
                             WHERE a.address = b.address
                             AND a.record>=b.record
                             GROUP BY 1
                             ")

## number of different full names for the address at the current record
fullnamePerAddr = dbGetQuery(app, 
                             "SELECT a.record,
                             COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname ELSE NULL END) -1 AS fullnamePerAddr_3,
                             COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname ELSE NULL END) -1 AS fullnamePerAddr_7,
                             COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname ELSE NULL END) -1 AS fullnamePerAddr_14,
                             COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 20 THEN b.fullname ELSE NULL END) -1 AS fullnamePerAddr_21,
                             COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 27 THEN b.fullname ELSE NULL END) -1 AS fullnamePerAddr_28
                             FROM data a, data b
                             WHERE a.date - b.date BETWEEN -29 AND 29 
                             AND a.address = b.address
                             GROUP BY 1")

## number of different full names for the phone at the current record
fullnamePerPhone = dbGetQuery(app, 
                              "SELECT a.record,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname ELSE NULL END) -1 AS fullnamePerPhone_3,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname ELSE NULL END) -1 AS fullnamePerPhone_7,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname ELSE NULL END) -1 AS fullnamePerPhone_14,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 20 THEN b.fullname ELSE NULL END) -1 AS fullnamePerPhone_21,
                              COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 27 THEN b.fullname ELSE NULL END) -1 AS fullnamePerPhone_28
                              FROM data a, data b
                              WHERE a.date - b.date BETWEEN 0 AND 29 
                              AND a.homephone = b.homephone
                              GROUP BY 1")

## number of different full names for the zip code at the current record
fullnamePerZip = dbGetQuery(app, 
                            "SELECT a.record,
                            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 THEN b.fullname ELSE NULL END) -1 AS fullnamePerZip_3,
                            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 THEN b.fullname ELSE NULL END) -1 AS fullnamePerZip_7,
                            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 THEN b.fullname ELSE NULL END) -1 AS fullnamePerZip_14,
                            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 20 THEN b.fullname ELSE NULL END) -1 AS fullnamePerZip_21,
                            COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 27 THEN b.fullname ELSE NULL END) -1 AS fullnamePerZip_28
                            FROM data a, data b
                            WHERE a.date - b.date BETWEEN 0 AND 29 
                            AND a.zip5 = b.zip5
                            GROUP BY 1")

join1 = left_join(fullnamePerAddr,fullnamePerPhone,c("record"="record"))

join2 = left_join(join1,fullnamePerZip,c("record"="record"))

## combine newly created variables into one dataset
new_vars=cbind.data.frame(data[-c(23559,41117),c(1,10,3,9)],same_ssn[-c(23559,41117),-1],same_fullname[-c(23559,41117),-1],same_address[-c(23559,41117),-1],same_zip[-c(23559,41117),-1],same_phone[-c(23559,41117),-1])
fwrite(new_vars, file = "new_vars.csv")

# Further clean the dataset to exclude outlier effect
dat = new_vars
colnames(dat)

## Remove identified outliers
wtssn = dat%>%
  filter(ssn!=737610282)
wtphone = dat%>%
  filter(homephone!=9105580920)

## Calculate mean for each column
ssn_set = c(5:29,135)
ssn_avg = colMeans(wtssn[,c(5:29,135)])
phone_set = c(110:134,136)
phone_avg = colMeans(wtphone[,c(110:134,136)])

## Change frivolous values to the mean of the column 
for (i in seq_along(ssn_set)){
  dat[dat$ssn==737610282,ssn_set[i]] = ssn_avg[i]
}

for (i in seq_along(phone_set)){
  dat[dat$homephone==9105580920,phone_set[i]] = phone_avg[i]
}

# Separate training and testing datasets
OOT = dat %>%
  filter(dat$record>=77851)
model_data = dat %>%
  filter(dat$record<77851)
set.seed(1)
train=sample(77848,62278)
training=model_data[train,-2]
testing=model_data[-train,-2]
