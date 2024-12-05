#------------------------------------------------------------------------------------------
### Setting up session
#------------------------------------------------------------------------------------------

library(dplyr)
library(sparklyr)
library(lubridate)
library(stringr)
library(tidyverse)
library(DBI)
library(openxlsx)
options(sparklyr.dplyr_distinct.impl = "tbl_lazy")
options(scipen = 999)

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
config$spark.yarn.executor.memoryOverhead <- "2g"
config$spark.executor.cores <- 5
config$spark.sql.shuffle.partitions <- 240

sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")

#------------------------------------------------------------------------------------------
### Load/set files 
#------------------------------------------------------------------------------------------

source("<filepath>/functions.R")
source("<filepath>/config.R")
outputs_folder <- <filepath>


linked_data_date <- "20240911"


for (hes_dataset in c("bariatric", "census_non_bariatric")){

# name of final dataset to save out
table_name <- paste0('hmrc_outcome.', hes_dataset, '_linked_monthly_hmrc_', format(Sys.Date(), '%Y%m%d'))


# name of sample flow file to save out
sample_flow_name <- paste0(outputs_folder,"sample_flow_linked_monthly_hmrc_", hes_dataset, "_", format(Sys.Date(), '%Y%m%d'), ".csv")

  
#------------------------------------------------------------------------------------------
# 2.3 Load linked data
#------------------------------------------------------------------------------------------

if (hes_dataset == "bariatric"){
  analytical_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.bariatric_linked_", linked_data_date))
}else if (hes_dataset == "census_non_bariatric"){
  analytical_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.census_non_bariatric_linked_sample_", linked_data_date))
}

n_rows <- as.character(sdf_nrow(analytical_df))
n_people <- as.character(sdf_nrow(distinct(analytical_df, census_id_final)))
sample_flow_linked <- data.frame(stage = c("Total number of rows in filtered, linked HES, deaths, Census",
                                    "Total number of people in filtered, linked HES, deaths, Census"),
                          count = c(n_rows, n_people))


#------------------------------------------------------------------------------------------
# Link to nino
#------------------------------------------------------------------------------------------

# Nino
# drop entries where same nino has multiple census id but 
# keep entries where same census id has multiple nino
nino_21 <- sdf_sql(sc, "SELECT frameworks_nino, census_id 
  FROM 2021_cen_link_encrypt_nino.census_di_nino_02102023
  WHERE nino_contains_multiple_census_id == 0")
nino_11 <- sdf_sql(sc, "SELECT frameworks_nino, census_id 
  FROM c11_linked_encrypted_nino.census_2011_di_nino_17112023
  WHERE nino_contains_multiple_census_id == 0")

nino_21 <- rename(nino_21, census_id_final = census_id,
                     valid_nino = frameworks_nino)

nino_11 <- rename(nino_11, census_id_final = census_id,
                     valid_nino = frameworks_nino)


# make 2011 census dataset
link_2011 <- analytical_df %>% filter(census_id_flag ==1) 

link_2011 <- inner_join(link_2011, nino_11, by = 'census_id_final')


link_2021 <- analytical_df %>% filter(census_id_flag ==0)

link_2021 <- inner_join(link_2021, nino_21, by = 'census_id_final')

# combine 2011 and 2021
linked_data_nhs_cen_nino <- rbind(link_2011, link_2021)  


n_rows <- as.character(sdf_nrow(linked_data_nhs_cen_nino))
n_people <- as.character(sdf_nrow(distinct(linked_data_nhs_cen_nino, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Total number of rows in linked HES, deaths, Census, nino",
                                                    "Total number of people in linked HES, deaths, Census, nino"),
                          count = c(n_rows, n_people)))

  
#------------------------------------------------------------------------------------------
# Make monthly dataset
#------------------------------------------------------------------------------------------


#Correct age bands
linked_data_nhs_cen_nino <- correct_age_bands(linked_data_nhs_cen_nino)

# make monthly dataset
monthly_person_level_df <- main_monthly_function(linked_data_nhs_cen_nino)

n_rows <- as.character(sdf_nrow(monthly_person_level_df))
n_people <- as.character(sdf_nrow(distinct(monthly_person_level_df, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows in monthly dataset",
                                                    "Number of people in monthly dataset"),
                          count = c(n_rows, n_people)))



#------------------------------------------------------------------------------------------
## 3.2 Link on HMRC
#------------------------------------------------------------------------------------------

hmrc_monthly <- sdf_sql(sc,"SELECT * FROM hmrc_outcome.monthly_pay_person_agg_test_20240710") #aggregated monthly data from DDU pipline

# create flag for has an entry in PAYE
hmrc_monthly <- hmrc_monthly %>%
  mutate(paye_flag = 1)

#rename census id in hmrc 
linked_data <- left_join(monthly_person_level_df, hmrc_monthly, 
                          by = c("valid_nino" = "valid_nino", 
                                 "month_label" = "month"))

# impute NA and negative pay as zero
# impute NA days worked as zero
# set non PAYE data to have PAYE flag 0
linked_data <- linked_data %>% 
  na.replace(pay = 0, days_worked = 0) %>%
  mutate(pay = ifelse(pay < 0, 0, pay),
         paye_flag = ifelse(is.na(paye_flag), 0, paye_flag))

n_rows <- as.character(sdf_nrow(linked_data))
n_people <- as.character(sdf_nrow(distinct(linked_data, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows in monthly dataset linked to PAYE",
                                                    "Number of people in monthly dataset linked to PAYE"),
                          count = c(n_rows, n_people)))


# aggregate pay for people with 2 national insurance numbers
linked_data <- linked_data %>%
 group_by(month_label,census_id_final)%>%
  mutate(pay= sum(pay),
         paye_flag = sum(paye_flag),
        days_worked = sum(days_worked)) %>% 
  mutate(paye_flag = ifelse(paye_flag >= 1, 1, 0)) %>%
  select(-c(valid_nino, rti_scheme_sk, start_work_date, payment_date)) %>%
  sdf_drop_duplicates() 

n_rows <- as.character(sdf_nrow(linked_data))
n_people <- as.character(sdf_nrow(distinct(linked_data, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows after combining multiple nino for same person",
                                                    "Number of people after combining multiple nino for same person"),
                          count = c(n_rows, n_people)))

write.csv(sample_flow_linked, sample_flow_name, row.names=FALSE)


#------------------------------------------------------------------------------------------
### Join births to monthly data 
#-----------------------------------------------------------------------------------------

# births data
births_wide <- sdf_sql(sc, "SELECT * FROM hmrc_outcome.births_wide_20240724")

# join on births date columns
linked_data <- left_join(linked_data, births_wide, 
                          by = c("census_id_final" = "baby_dob_census_id_final"))

#-----------------------------------------------------------------------------------------
### Create derived variables
#-----------------------------------------------------------------------------------------

# birth within last year
# birth_in_1yr is 1 if birth is in ref month or in 11 months previous
linked_data <- linked_data %>%
   mutate(birth_in_1yr = ifelse((!is.na(baby_dob_1) & months_between(ref_date, baby_dob_1) < 12 & months_between(ref_date, baby_dob_1) >= 0) | 
                                (!is.na(baby_dob_2) & months_between(ref_date, baby_dob_2) < 12 & months_between(ref_date, baby_dob_2) >= 0) | 
                                (!is.na(baby_dob_3) & months_between(ref_date, baby_dob_3) < 12 & months_between(ref_date, baby_dob_3) >= 0) | 
                                (!is.na(baby_dob_4) & months_between(ref_date, baby_dob_4) < 12 & months_between(ref_date, baby_dob_4) >= 0) | 
                                (!is.na(baby_dob_5) & months_between(ref_date, baby_dob_5) < 12 & months_between(ref_date, baby_dob_5) >= 0) | 
                                (!is.na(baby_dob_6) & months_between(ref_date, baby_dob_6) < 12 & months_between(ref_date, baby_dob_6) >= 0) | 
                                (!is.na(baby_dob_7) & months_between(ref_date, baby_dob_7) < 12 & months_between(ref_date, baby_dob_7) >= 0) | 
                                (!is.na(baby_dob_8) & months_between(ref_date, baby_dob_8) < 12 & months_between(ref_date, baby_dob_8) >= 0) | 
                                (!is.na(baby_dob_9) & months_between(ref_date, baby_dob_9) < 12 & months_between(ref_date, baby_dob_9) >= 0) | 
                                (!is.na(baby_dob_10) & months_between(ref_date, baby_dob_10) < 12 & months_between(ref_date, baby_dob_10) >= 0) | 
                                (!is.na(baby_dob_11) & months_between(ref_date, baby_dob_11) < 12 & months_between(ref_date, baby_dob_11) >= 0), 1, 0))


# birth within last 1-5 years
# birth_in_5yr is 1 if birth is in ref month or in 59 months previous, not including the first year
linked_data <- linked_data %>%
   mutate(birth_in_5yr = ifelse((!is.na(baby_dob_1) & months_between(ref_date, baby_dob_1) < 60 & months_between(ref_date, baby_dob_1) >= 12) | 
                                (!is.na(baby_dob_2) & months_between(ref_date, baby_dob_2) < 60 & months_between(ref_date, baby_dob_2) >= 12) | 
                                (!is.na(baby_dob_3) & months_between(ref_date, baby_dob_3) < 60 & months_between(ref_date, baby_dob_3) >= 12) | 
                                (!is.na(baby_dob_4) & months_between(ref_date, baby_dob_4) < 60 & months_between(ref_date, baby_dob_4) >= 12) | 
                                (!is.na(baby_dob_5) & months_between(ref_date, baby_dob_5) < 60 & months_between(ref_date, baby_dob_5) >= 12) | 
                                (!is.na(baby_dob_6) & months_between(ref_date, baby_dob_6) < 60 & months_between(ref_date, baby_dob_6) >= 12) | 
                                (!is.na(baby_dob_7) & months_between(ref_date, baby_dob_7) < 60 & months_between(ref_date, baby_dob_7) >= 12) | 
                                (!is.na(baby_dob_8) & months_between(ref_date, baby_dob_8) < 60 & months_between(ref_date, baby_dob_8) >= 12) | 
                                (!is.na(baby_dob_9) & months_between(ref_date, baby_dob_9) < 60 & months_between(ref_date, baby_dob_9) >= 12) | 
                                (!is.na(baby_dob_10) & months_between(ref_date, baby_dob_10) < 60 & months_between(ref_date, baby_dob_10) >= 12) | 
                                (!is.na(baby_dob_11) & months_between(ref_date, baby_dob_11) < 60 & months_between(ref_date, baby_dob_11) >= 12), 1, 0))

# drop columns
linked_data <- linked_data %>%
  select(-contains('baby_dob_'))


#------------------------------------------------------------------------------------------
### SAVE OUT FINAL LINKED DATASET
#------------------------------------------------------------------------------------------ 

tbl_change_db(sc, "hmrc_outcome")

sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(linked_data , 'linked_data')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM linked_data')
DBI::dbExecute(sc, sql)
  
}
