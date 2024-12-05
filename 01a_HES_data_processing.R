# Produces the HES, person-level datasets onto which other datasets will be linked
# These are the main analysis dataset of people who had bariatric surgery
# non exposed dataset of people in the Census 2011 who did not have bariatric surgery


#------------------------------------------------------------------------------------------
# Setting up session
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
# Load/set files 
#------------------------------------------------------------------------------------------

source("<filepath>/functions.R")
source("<filepath>/config.R")
outputs_folder <- <filepath>

#######################################################################################
#                  CREATE EPISODE LEVEL DATASET FOR ALL DATASETS                      #
#                                                                                     #
#                  processing dataset contains all APC records                        #
#                  between 1 April 2014 to 31 December 2022 at episode level          #
#                  for people with a Census ID                                        #
#                                                                                     #                                                                #
#######################################################################################

#------------------------------------------------------------------------------------------
# Load in Bariatric procedure, diagnosis and condition codes
#------------------------------------------------------------------------------------------
bariatric_procedure_codes <- read.csv('<filepath>/bariatric_opcs_lookup.csv')
bariatric_diag_codes <- read.csv('<filepath>/bariatric_icd_lookup.csv')
associated_cond_codes <- read.csv('<filepath>/bariatric_associated_cond_lookup.csv')

procedure_codes <- bariatric_procedure_codes[[2]]
diag_codes <- bariatric_diag_codes[[2]]
associated_codes <- associated_cond_codes[[2]]

#------------------------------------------------------------------------------------------
# Load HES data, add flags
#------------------------------------------------------------------------------------------

df_hes_apc <- sdf_sql(sc, ("SELECT epikey, epistart, epiend,opertn_4_01, opertn_4_02, opertn_4_03, opertn_4_04, 
                      opertn_4_05, opertn_4_06, opertn_4_07, opertn_4_08, opertn_4_09, opertn_4_10, opertn_4_11, opertn_4_12, opertn_4_13, 
                      opertn_4_14, opertn_4_15, opertn_4_16, opertn_4_17, opertn_4_18, opertn_4_19, opertn_4_20, opertn_4_21, opertn_4_22, 
                      opertn_4_23, opertn_4_24, opdate_01, opdate_02, opdate_03, opdate_04, opdate_05, opdate_06, opdate_07, opdate_08, opdate_09, 
                      opdate_10, opdate_11, opdate_12, opdate_13, opdate_14, opdate_15, opdate_16, opdate_17, opdate_18, opdate_19, opdate_20, opdate_21, 
                      opdate_22, opdate_23, opdate_24, diag_4_01,diag_4_02, diag_4_03, diag_4_04,diag_4_05,diag_4_06,diag_4_07,diag_4_08,diag_4_09,
                   diag_4_10, diag_4_11,diag_4_12,diag_4_13,diag_4_14,diag_4_15,diag_4_16,diag_4_17,diag_4_18,diag_4_19,
                   diag_4_20,admiage, startage, sex, ethnos,lsoa11, census_person_id_2011, census_id_2021, datediff(epiend, epistart) as date_diff
                      FROM deident_hospital_episode_stats.deidentified_hes_inpatient_pipeline_module_data_v3"))

## create flags for filtering on in processing dataset
# if value is NA, then set flag to 0
processing_df_all <- df_hes_apc %>% mutate(primary_obesity_flag = case_when(diag_4_01 %in% diag_codes ~ 1,
                                                               TRUE ~ 0),
                                secondary_obesity_flag = case_when(!diag_4_01 %in% diag_codes & (diag_4_02 %in% diag_codes | diag_4_03 %in% diag_codes |
                                      diag_4_04 %in% diag_codes | diag_4_05 %in% diag_codes | diag_4_06 %in% diag_codes |
                                      diag_4_07 %in% diag_codes | diag_4_08 %in% diag_codes | diag_4_09 %in% diag_codes |
                                      diag_4_10 %in% diag_codes | diag_4_11 %in% diag_codes | diag_4_12 %in% diag_codes |
                                      diag_4_13 %in% diag_codes | diag_4_14 %in% diag_codes | diag_4_15 %in% diag_codes |
                                      diag_4_16 %in% diag_codes | diag_4_17 %in% diag_codes | diag_4_18 %in% diag_codes |
                                      diag_4_19 %in% diag_codes | diag_4_20 %in% diag_codes) ~ 1,
                                                                TRUE ~ 0),
                              associated_cond_flag = case_when(diag_4_01 %in% associated_codes ~ 1,
                                                              TRUE ~ 0),
                              metabolic_op_flag = case_when(opertn_4_01 %in% procedure_codes | opertn_4_02 %in% procedure_codes | opertn_4_03 %in% procedure_codes |
                                      opertn_4_04 %in% procedure_codes | opertn_4_05 %in% procedure_codes | opertn_4_06 %in% procedure_codes |
                                      opertn_4_07 %in% procedure_codes | opertn_4_08 %in% procedure_codes | opertn_4_09 %in% procedure_codes |
                                      opertn_4_10 %in% procedure_codes | opertn_4_11 %in% procedure_codes | opertn_4_12 %in% procedure_codes |
                                      opertn_4_13 %in% procedure_codes | opertn_4_14 %in% procedure_codes | opertn_4_15 %in% procedure_codes |
                                      opertn_4_16 %in% procedure_codes | opertn_4_17 %in% procedure_codes | opertn_4_18 %in% procedure_codes |
                                      opertn_4_19 %in% procedure_codes | opertn_4_20 %in% procedure_codes | opertn_4_21 %in% procedure_codes |
                                      opertn_4_22 %in% procedure_codes | opertn_4_23 %in% procedure_codes | opertn_4_24 %in% procedure_codes ~ 1,
                                                           TRUE ~0),
                              census_id_flag = case_when(!is.na(census_person_id_2011) ~ 1,
                                                   TRUE ~ 0))

# convert dates
processing_df_all <- processing_df_all %>%
                 mutate(epistart = to_date(epistart,"yyyy-MM-dd")) %>%
                 mutate(epiend = to_date(epiend, "yyyy-MM-dd"))


# create census id final - we want to use 2011 as a base, if no 2011 id then use 2021
processing_df_all <- processing_df_all %>% mutate(census_id_final = ifelse(census_id_flag ==1, census_person_id_2011, census_id_2021))


# previous surgery flag (episode level)
processing_df_all <- processing_df_all %>% 
  mutate(previous_surgery = case_when(epistart < follow_up_start_date & metabolic_op_flag == 1 ~ 1, 
                                              TRUE ~ 0))
# any previous surgery flag (person level)
processing_df_all <- processing_df_all %>%
  group_by(census_id_final) %>%
  mutate(previous_surgery_total = sum(previous_surgery)) %>%
  ungroup() %>%
  mutate(any_prev_metab_surgery_flag = ifelse(previous_surgery_total>=1, 1, 0)) %>%
  select(-previous_surgery_total)


# surgery in follow up time flag (episode level)
processing_df_all <- processing_df_all %>% 
  mutate(surgery_in_follow_up = case_when(epistart >= follow_up_start_date & epiend < follow_up_end_date & metabolic_op_flag == 1 ~ 1, 
                                              TRUE ~ 0))

# any surgery in follow up time flag (person level)
processing_df_all <- processing_df_all %>%
  group_by(census_id_final) %>%
  mutate(metabolic_surgery_total = sum(surgery_in_follow_up)) %>%
  ungroup() %>%
  mutate(any_metabolic_op_flag_in_dates = ifelse(metabolic_surgery_total>=1, 1, 0)) %>%
  select(-metabolic_surgery_total)


#------------------------------------------------------------------------------------------
# Filter HES data to has census id
#------------------------------------------------------------------------------------------

# save dataset sizes
sample_flow <- initialise_sample_flow()
sample_flow <- add_to_sample_flow(processing_df_all, sample_flow, 
                                  row_label = "Total number of episodes in deidentified HES APC",
                                  id_col = census_id_final,
                                  id_label = "Total number of people in deidentified HES APC")


# filter out people with no census id
processing_df_all <- processing_df_all %>%
  filter(!is.na(census_id_final))

sample_flow <- add_to_sample_flow(processing_df_all, sample_flow, 
                                  row_label = "Number of episodes with censusID",
                                  id_col = census_id_final,
                                  id_label = "Number of people with censusID")



#------------------------------------------------------------------------------------------
# Filter HES data to date range for datasets that need episodes in follow up only
#------------------------------------------------------------------------------------------

# filter to date range of surgeries needed
processing_df <- processing_df_all %>%
  filter(epistart >= follow_up_start_date & epiend < follow_up_end_date & epiend > "1802-01-01")

# save dataset sizes
sample_flow <- add_to_sample_flow(processing_df, sample_flow, 
                                  row_label = "Total number of episodes in follow up time",
                                  id_col = census_id_final,
                                  id_label = "Total number of people in follow up time")


# save data range
min_max <- processing_df %>% 
  summarise(min_epiend = min(epiend), max_epiend = max(epiend), 
            min_epistart = min(epistart), max_epistart = max(epistart)) %>%
  collect()

sample_flow <- add_custom_rows_to_sample_flow(sample_flow,
                                              row_labels = c("Min epiend in deidentified HES APC",
                                "Max epiend in deidentified HES APC",
                                "Min epistart in deidentified HES APC",
                                "Max epistart in deidentified HES APC"),
                          counts = c(as.character(min_max$min_epiend),
                                   as.character(min_max$max_epiend),
                                   as.character(min_max$min_epistart),
                                   as.character(min_max$max_epistart)))







#######################################################################################
#           CREATE PERSON LEVEL DATASET FOR MAIN ANALYSIS                             #
#                                                                                     #
#           bariatric dataset contains people who had a bariatric surgery             #
#           from 1 April 2014 to 31 March 2021, who did not have bariatric surgery    #
#           in the five previous years and have a primary diagnosis of obesity or     #
#           a secondayr diagnosis of obesity with a primary diagnosis of an obesity   #
#           related condition                                                         #
#                                                                                     #                                                                #
#######################################################################################


#------------------------------------------------------------------------------------------
# Filter to main dataset: bariatric surgery and obesity, no previous bariatric surgery
#------------------------------------------------------------------------------------------

# filter to episodes of surgery in the date range 
bariatric_df <- processing_df %>%
  filter(metabolic_op_flag == 1)


sample_flow <- add_to_sample_flow(bariatric_df, sample_flow, 
                                  row_label = "Number of episodes with bariatric surgery in follow up",
                                  id_col = census_id_final,
                                  id_label = "Number of people with bariatric surgery in follow up")

# filter out people who had a previous surgery
bariatric_df <- bariatric_df %>%
  filter(any_prev_metab_surgery_flag==0)


sample_flow <- add_to_sample_flow(bariatric_df, sample_flow, 
                                  row_label = "Number of episodes without previous bariatric surgery",
                                  id_col = census_id_final,
                                  id_label = "Number of people without previous bariatric surgery")


# filter to those with primary obesity diagnosis or secondary obesity diagnosis with primary associated condition
# at the same episode as the surgery
bariatric_df <- bariatric_df %>%
  filter(primary_obesity_flag ==1 | (secondary_obesity_flag ==1 & associated_cond_flag == 1) )

sample_flow <- add_to_sample_flow(bariatric_df, sample_flow, 
                                  row_label = "Number of episodes with primary obesity or secondary obesity with primary associated contition diagnosis",
                                  id_col = census_id_final,
                                  id_label = "Number of people with primary obesity or secondary obesity with primary associated contition diagnosis")



#------------------------------------------------------------------------------------------
# Process HES data - create derived variables
#------------------------------------------------------------------------------------------


# for people with multiple surgery codes for bariatric surgery, identify the first
# to use as their surgery type.
# use primary code for people with primary 
bariatric_df <- bariatric_df %>%
  mutate(op_code = case_when(opertn_4_01 %in% procedure_codes ~ opertn_4_01,
                                   opertn_4_02 %in% procedure_codes ~ opertn_4_02,
                                   opertn_4_03 %in% procedure_codes ~ opertn_4_03,
                                   opertn_4_04 %in% procedure_codes ~ opertn_4_04,
                                   opertn_4_05 %in% procedure_codes ~ opertn_4_05,
                                   opertn_4_06 %in% procedure_codes ~ opertn_4_06,
                                   opertn_4_07 %in% procedure_codes ~ opertn_4_07,
                                   opertn_4_08 %in% procedure_codes ~ opertn_4_08,
                                   opertn_4_09 %in% procedure_codes ~ opertn_4_09,
                                   opertn_4_10 %in% procedure_codes ~ opertn_4_10,
                                   opertn_4_11 %in% procedure_codes ~ opertn_4_11,
                                   opertn_4_12 %in% procedure_codes ~ opertn_4_12,
                                   opertn_4_13 %in% procedure_codes ~ opertn_4_13,
                                   opertn_4_14 %in% procedure_codes ~ opertn_4_14,
                                   opertn_4_15 %in% procedure_codes ~ opertn_4_15,
                                   opertn_4_16 %in% procedure_codes ~ opertn_4_16,
                                   opertn_4_17 %in% procedure_codes ~ opertn_4_17,
                                   opertn_4_18 %in% procedure_codes ~ opertn_4_18,
                                   opertn_4_19 %in% procedure_codes ~ opertn_4_19,
                                   opertn_4_20 %in% procedure_codes ~ opertn_4_20,
                                   opertn_4_21 %in% procedure_codes ~ opertn_4_21,
                                   opertn_4_22 %in% procedure_codes ~ opertn_4_22,
                                   opertn_4_23 %in% procedure_codes ~ opertn_4_23,
                                   opertn_4_24 %in% procedure_codes ~ opertn_4_24))

# identify operation type from the first relevant operation code
bariatric_procedure_codes_spk <- copy_to(sc, bariatric_procedure_codes, overwrite = TRUE)

bariatric_df <- left_join(bariatric_df, bariatric_procedure_codes_spk, 
                           by=c("op_code" = "Code")) %>%
  rename(operation_type = Name)


# get date of operation corresponding to the operation for bariatric surgery
# identified above
bariatric_df <- bariatric_df %>%
  mutate(op_date = case_when(opertn_4_01 %in% procedure_codes ~ opdate_01,
                                   opertn_4_02 %in% procedure_codes ~ opdate_02,
                                   opertn_4_03 %in% procedure_codes ~ opdate_03,
                                   opertn_4_04 %in% procedure_codes ~ opdate_04,
                                   opertn_4_05 %in% procedure_codes ~ opdate_05,
                                   opertn_4_06 %in% procedure_codes ~ opdate_06,
                                   opertn_4_07 %in% procedure_codes ~ opdate_07,
                                   opertn_4_08 %in% procedure_codes ~ opdate_08,
                                   opertn_4_09 %in% procedure_codes ~ opdate_09,
                                   opertn_4_10 %in% procedure_codes ~ opdate_10,
                                   opertn_4_11 %in% procedure_codes ~ opdate_11,
                                   opertn_4_12 %in% procedure_codes ~ opdate_12,
                                   opertn_4_13 %in% procedure_codes ~ opdate_13,
                                   opertn_4_14 %in% procedure_codes ~ opdate_14,
                                   opertn_4_15 %in% procedure_codes ~ opdate_15,
                                   opertn_4_16 %in% procedure_codes ~ opdate_16,
                                   opertn_4_17 %in% procedure_codes ~ opdate_17,
                                   opertn_4_18 %in% procedure_codes ~ opdate_18,
                                   opertn_4_19 %in% procedure_codes ~ opdate_19,
                                   opertn_4_20 %in% procedure_codes ~ opdate_20,
                                   opertn_4_21 %in% procedure_codes ~ opdate_21,
                                   opertn_4_22 %in% procedure_codes ~ opdate_22,
                                   opertn_4_23 %in% procedure_codes ~ opdate_23,
                                   opertn_4_24 %in% procedure_codes ~ opdate_24))


#------------------------------------------------------------------------------------------
# Imputation of operation date
#------------------------------------------------------------------------------------------

# count of opdates to impute
n_sys_opdate <- bariatric_df %>% filter(op_date < "1850-01-01") %>% sdf_nrow()

# test what to impute for opdates that are system dates
days_epistart_to_surgery <- bariatric_df %>%
  mutate(diff_op_epistart = datediff(op_date, epistart)) %>%
  group_by(diff_op_epistart) %>%
  count() %>%
  collect() %>%
  data.frame() %>%
  arrange(diff_op_epistart)

# Save imputation of opdate info
dataset_names <- list('n_sys_opdate' = n_sys_opdate,
                     'days_epistart_to_surgery' = days_epistart_to_surgery)
write.xlsx(dataset_names, 
           paste0(outputs_folder,"op_date_imputation_info.xlsx"))


# impute system optates to be epistart as majority of operations are on day of surgery
bariatric_df <- bariatric_df %>%
                mutate(op_date = ifelse(op_date < "1850-01-01", epistart, op_date)) %>%
                select(-opdate_01, -opdate_02, -opdate_03, -opdate_04, -opdate_05, -opdate_06, -opdate_07, -opdate_08, -opdate_09, 
                      -opdate_10, -opdate_11, -opdate_12, -opdate_13, -opdate_14, -opdate_15, -opdate_16, -opdate_17, -opdate_18, -opdate_19, -opdate_20, -opdate_21, 
                      -opdate_22, -opdate_23, -opdate_24,-opertn_4_01, -opertn_4_02, -opertn_4_03, -opertn_4_04, 
                      -opertn_4_05, -opertn_4_06, -opertn_4_07, -opertn_4_08, -opertn_4_09, -opertn_4_10, -opertn_4_11, -opertn_4_12, -opertn_4_13, 
                      -opertn_4_14, -opertn_4_15, -opertn_4_16, -opertn_4_17, -opertn_4_18, -opertn_4_19, -opertn_4_20, -opertn_4_21, -opertn_4_22, 
                      -opertn_4_23, -opertn_4_24)

#------------------------------------------------------------------------------------------
# Create person level dataset and deduplicate
#------------------------------------------------------------------------------------------

# Convert dates
analytical_df <- bariatric_df  %>% 
                 mutate(op_date = to_date(op_date, "yyyy-MM-dd"))

# Select only the index operation 
analytical_df <- analytical_df %>%
  group_by(census_id_final) %>%
  filter(epistart == min(epistart) & epiend == min(epiend)) %>%
  ungroup()

# Drop any duplicate instances e.g. where data is the same except different epikey so we get one per person
# Working on the assumption that any instances where someone has two epikeys with exactly the same dates, 
# that the larger epikey is the most recent and most accurate/contains more information on diag codes
analytical_df <- analytical_df %>%
  group_by(census_id_final, epistart, epiend) %>%
  filter(op_date == min(op_date)) %>%
  filter(epikey == max(epikey)) %>%
  ungroup() 


sample_flow <- add_to_sample_flow(analytical_df, sample_flow, 
                                  row_label = "Number of episodes after deduplication",
                                  id_col = census_id_final,
                                  id_label = "Number of people after deduplication")


write.csv(sample_flow, paste0(outputs_folder,"HES_sample_flow_bariatric_", Sys.Date(), ".csv"),
         row.names = FALSE)


#------------------------------------------------------------------------------------------
# Save HES dataset
#------------------------------------------------------------------------------------------
                         
table_name = paste0('hmrc_outcome.bariatric_hes_', format(Sys.Date(), '%Y%m%d'))
tbl_change_db(sc, "hmrc_outcome")

#delete table if already exists 
sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(analytical_df , 'analytical_df')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM analytical_df')
DBI::dbExecute(sc, sql)





########################################################################################
#           CREATE PERSON LEVEL DATASET FOR ALL WITH NO BARIATRIC SURGERY DATASET      #
#                                                                                      #
#           non bariatric dataset contains people who are in the 2011 census (but no   #
#           requirement to be in HES) who did not have a bariatric surgery             #
#           from 1 April 2014 to 31 March 2021 and who did not have bariatric surgery  #
#           in the five previous years                                                 #
#                                                                                      #                                                                #
########################################################################################

#------------------------------------------------------------------------------------------
# Read in Census 2021 ids
#------------------------------------------------------------------------------------------

census_non_bariatric_df <- sdf_sql(sc,  "SELECT il4_person_id
                          FROM deidentified_census_2011.deidentified_census_2011")

sample_flow_census_non_bariatric <- initialise_sample_flow()
sample_flow_census_non_bariatric <- add_to_sample_flow(census_non_bariatric_df, sample_flow_census_non_bariatric, 
                                  row_label = "Total number of rows in 2011 Census",
                                  id_col = il4_person_id,
                                  id_label = "Total number of people in 2011 Census")

#------------------------------------------------------------------------------------------
# Link on HES and filter out bariatric surgery
#------------------------------------------------------------------------------------------

# get person level HES with flags and lsoa as an empty column
processing_df_to_join <- processing_df_all %>%
  select(census_person_id_2011, any_prev_metab_surgery_flag, any_metabolic_op_flag_in_dates) %>%
  filter(!is.na(census_person_id_2011)) %>%
  distinct() %>%
  sdf_drop_duplicates()


# join onto census
census_non_bariatric_df <- left_join(census_non_bariatric_df, processing_df_to_join, 
                                              by=c("il4_person_id"="census_person_id_2011"))

# rename census id column
census_non_bariatric_df <- census_non_bariatric_df %>%
  rename(census_id_final = il4_person_id)

# add census_id_flag columns
census_non_bariatric_df <- census_non_bariatric_df %>%
  mutate(census_id_flag = 1,
         any_metabolic_op_flag_in_dates = ifelse(is.na(any_metabolic_op_flag_in_dates), 0, any_metabolic_op_flag_in_dates),
         any_prev_metab_surgery_flag = ifelse(is.na(any_prev_metab_surgery_flag), 0, any_prev_metab_surgery_flag))

# have not had any bariatric surgery in the date range
census_non_bariatric_df <- census_non_bariatric_df %>% 
  filter(is.na(any_metabolic_op_flag_in_dates) |  any_metabolic_op_flag_in_dates== 0) 

sample_flow_census_non_bariatric <- add_to_sample_flow(census_non_bariatric_df, sample_flow_census_non_bariatric, 
                                  row_label = "Total number of rows with no bariatric surgery in follow up",
                                  id_col = census_id_final,
                                  id_label = "Total number of people with no bariatric surgery in follow up")


# has not had previous bariatric surgery
census_non_bariatric_df <- census_non_bariatric_df %>% 
  filter(is.na(any_prev_metab_surgery_flag) |  any_prev_metab_surgery_flag== 0) 

sample_flow_census_non_bariatric <- add_to_sample_flow(census_non_bariatric_df, sample_flow_census_non_bariatric, 
                                  row_label = "Number of rows with no previous metabolic surgery",
                                  id_col = census_id_final,
                                  id_label = "Number of people with no previous metabolic surgery")




#------------------------------------------------------------------------------------------
# Save out dataset
#------------------------------------------------------------------------------------------

table_name = paste0('hmrc_outcome.census_non_bariatric_hes_', format(Sys.Date(), '%Y%m%d'))
tbl_change_db(sc, "hmrc_outcome")

#delete table if already exists 
sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(census_non_bariatric_df , 'census_non_bariatric_df')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM census_non_bariatric_df')
DBI::dbExecute(sc, sql)

rm(census_non_bariatric_df)

write.csv(sample_flow_census_non_bariatric, paste0(outputs_folder,"HES_sample_flow_census_non_bariatric_", Sys.Date(), ".csv"),
         row.names = FALSE)



