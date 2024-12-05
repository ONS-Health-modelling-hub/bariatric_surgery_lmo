# read in linked, filtered bariatric dataset
# get distribution
# read in linked non exposed datasets
# add dates in correct distribution
# sample to match bariatric dataset 


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


###########################################################################################
#                                                                                         #
#                      Create strata proportions based on bariatric sample                #
#                                                                                         #
###########################################################################################

#------------------------------------------------------------------------------------------
### Load in bariatric data from linked file
#------------------------------------------------------------------------------------------

bariatric_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.bariatric_linked_", linked_data_date))

#------------------------------------------------------------------------------------------
### Generate strata proportions
#------------------------------------------------------------------------------------------

#Get proportions of age and sex stratas
bariatric_strata <- bariatric_df %>%
                    select(census_sex, age_at_operation) %>%
                    mutate(five_year_bands = case_when(age_at_operation >= 25 & age_at_operation < 30 ~ "25-29",
                                                       age_at_operation >= 30 & age_at_operation < 35 ~ "30-34",
                                                       age_at_operation >= 35 & age_at_operation < 40 ~ "35-39",
                                                       age_at_operation >= 40 & age_at_operation < 45 ~ "40-44",
                                                       age_at_operation >= 45 & age_at_operation < 50 ~ "45-49",
                                                       age_at_operation >= 50 & age_at_operation < 55 ~ "50-54",
                                                       age_at_operation >= 55 & age_at_operation < 60 ~ "55-59",
                                                       age_at_operation >= 60 & age_at_operation < 65 ~ "60-64")) %>%
                    group_by(census_sex, five_year_bands) %>%
                    summarise(n = n()) %>%
                    ungroup() %>%
                    mutate(prop = n/sum(n)) %>%
                    arrange(census_sex, five_year_bands) %>%
                    collect()

bariatric_strata_props <- bariatric_strata %>%
                          select(prop)

# Make into vector
strata_proportions <- pull(bariatric_strata_props)





###########################################################################################
#                                                                                         #
#                              Create non-exposure census, non bariatric sample           #
#                                                                                         #
###########################################################################################


linked_person_census_non_bariatric <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.census_non_bariatric_linked_", linked_data_date))

#------------------------------------------------------------------------------------------
## Get stratified sample non bariatric
#------------------------------------------------------------------------------------------

#Get proportions of age and sex stratas
linked_person_census_non_bariatric <- linked_person_census_non_bariatric %>%
               mutate(five_year_bands = case_when(age_at_operation < 20 ~ "Under 20",
                                                  age_at_operation >= 20 & age_at_operation < 25 ~ "21-24",
                                                  age_at_operation >= 25 & age_at_operation < 30 ~ "25-29",
                                                  age_at_operation >= 30 & age_at_operation < 35 ~ "30-34",
                                                  age_at_operation >= 35 & age_at_operation < 40 ~ "35-39",
                                                  age_at_operation >= 40 & age_at_operation < 45 ~ "40-44",
                                                  age_at_operation >= 45 & age_at_operation < 50 ~ "45-49",
                                                  age_at_operation >= 50 & age_at_operation < 55 ~ "50-54",
                                                       age_at_operation >= 55 & age_at_operation < 60 ~ "55-59",
                                                       age_at_operation >= 60 & age_at_operation < 65 ~ "60-64",
                                                 age_at_operation >= 65 ~ 'Over 65'))

n_required <- 50000

# save dataset sizes
sample_flow_census_non_bariatric <- initialise_sample_flow()
sample_flow_census_non_bariatric <- add_to_sample_flow(linked_person_census_non_bariatric, sample_flow_census_non_bariatric, 
                                  row_label = "Total number of rows in linked data",
                                  id_col = census_id_final,
                                  id_label = "Total number of people in linked data")

#test <- linked_person_census_non_bariatric %>%
#  group_by(census_sex, five_year_bands) %>%
#  summarise(n = n()) %>%
#  ungroup() %>%
#  collect() %>%
#  left_join(select(bariatric_strata, five_year_bands, census_sex, prop), 
#            by=c("census_sex", "five_year_bands")) %>%
#  arrange(census_sex, five_year_bands) %>%
#  mutate(n_needed = n_required*prop) %>%
#  mutate(not_enough_data = ifelse(n_needed>n, 1, 0))
#data.frame(test)

census_non_bariatric_sample <- create_strata_sample(linked_person_census_non_bariatric, n_required)

census_non_bariatric_sample <- census_non_bariatric_sample %>%
  select(-five_year_bands)

sample_flow_census_non_bariatric <- add_to_sample_flow(census_non_bariatric_sample, sample_flow_census_non_bariatric, 
                                  row_label = "Total number of rows in linked data after sampling",
                                  id_col = census_id_final,
                                  id_label = "Total number of people in linked data after sampling")

write.csv(sample_flow_census_non_bariatric, paste0(outputs_folder,"Sampling_sample_flow_census_non_bariatric_", Sys.Date(), ".csv"),
         row.names = FALSE)



#------------------------------------------------------------------------------------------
## Save out
#------------------------------------------------------------------------------------------

# Save to Hive
table_name = paste0('hmrc_outcome.census_non_bariatric_linked_sample_', format(Sys.Date(), '%Y%m%d'))
tbl_change_db(sc, "hmrc_outcome")

sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(census_non_bariatric_sample , 'census_non_bariatric_sample')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM census_non_bariatric_sample')
DBI::dbExecute(sc, sql)


rm(census_non_bariatric_sample)




######################################################################
