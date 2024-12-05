library(dplyr)
library(sparklyr)
library(lubridate)
library(stringr)
library(tidyverse)
library(DBI)
library(lfe)
library(splines)
library(survival)
library(openxlsx)
library(did)
library(lmtest)
library(gridExtra)

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
### Load functions and set files 
#------------------------------------------------------------------------------------------

source("<filepath>/modelling_functions.R")

dataset_date <- "20240827"

folder <- <filepath>

dir.create(file.path(folder, paste0("Results_", dataset_date)))

outputs_folder <- <filepath>

# create results folders
for (i in c("main_results", "pretreatment", "sensitivity_tests", "secondary_analyses",
          "testing_model_specs")){
  dir.create(file.path(outputs_folder, i))
}


#------------------------------------------------------------------------------------------
### Read in exposed data and collect, make treatment and outcome derived variables
#------------------------------------------------------------------------------------------

# main analysis sample
bariatric_linked_df <- sdf_sql(sc, paste0("SELECT census_id_final, t_op, month_label, census_sex, age_monthly, 
                                    pay, disability_binary, rural_urban, imd_quintile, region, age_bands,
                                    birth_in_1yr, birth_in_5yr, ethnic_group_tb, operation_type
                                    FROM hmrc_outcome.bariatric_linked_monthly_hmrc_", dataset_date))


bariatric_linked_df <- data.frame(bariatric_linked_df) %>%
  arrange(census_id_final, t_op)


df <- treatment_variables(bariatric_linked_df)
df <- outcome_variables(df)
df <- set_factors(df)


# variables for did function
df$treat <- 1

# add numerical calendar time column for every 6 months
df <- df %>%
  mutate(month_num = interval(as.Date("2014-04-01", format='%Y-%m-%d'),
                                        as.Date(paste0(month_label, "-01"), format='%Y-%m-%d')) %/% months(1) +1) %>%
  mutate(six_month_num = ceiling(month_num/6))


df <- df %>%
  group_by(census_id_final) %>%
  mutate(treatment_group = six_month_num[t_op == 1]) %>%
  ungroup()

df <- filter(df, t_op <= 60)

rm(bariatric_linked_df)

#------------------------------------------------------------------------------------------
### Read in unexposed census bariatric sample, make treatment and outcome derived variables
#------------------------------------------------------------------------------------------

# non exposed obese sample
non_bariatric_linked_df <- sdf_sql(sc, paste0("SELECT census_id_final, t_op, month_label, census_sex, age_monthly, 
                                    pay, days_worked, disability_binary, rural_urban, imd_decile, region, age_bands,
                                    birth_in_1yr, birth_in_5yr, ethnic_group_tb, imd_quintile
                                              FROM hmrc_outcome.census_non_bariatric_linked_monthly_hmrc_", dataset_date))

non_bariatric_linked_df <- data.frame(non_bariatric_linked_df) %>%
  arrange(census_id_final, t_op)

df_non_bariatric <- treatment_variables(non_bariatric_linked_df)
df_non_bariatric <- outcome_variables(df_non_bariatric)
df_non_bariatric <- set_factors(df_non_bariatric)

# add operation type so can join datasets
df_non_bariatric$operation_type <- "None"

# variables for did function
df_non_bariatric$treat <- 0
df_non_bariatric$treatment_group <- 0

# add numerical calendar time column for every 6 months
df_non_bariatric <- df_non_bariatric %>%
  mutate(month_num = interval(as.Date("2014-04-01", format='%Y-%m-%d'),
                                        as.Date(paste0(month_label, "-01"), format='%Y-%m-%d')) %/% months(1) +1) %>%
  mutate(six_month_num = ceiling(month_num/6))

rm(non_bariatric_linked_df)




#------------------------------------------------------------------------------------------
### Make combined exposed/non-exposed dataset
#------------------------------------------------------------------------------------------

# make treatment all zero for non bariatric
df_non_bariatric_for_combined <- df_non_bariatric %>%
  mutate(t_treatment_6_month = as.factor(0),
        t_treatment_6_month_baseline = as.factor(0))

df_combined_non_bariatric <- bind_rows(df, df_non_bariatric_for_combined)

df_combined_non_bariatric$t_treatment_6_month <- relevel(df_combined_non_bariatric$t_treatment_6_month, "0")

#df_combined_non_bariatric <- filter(df_combined_non_bariatric, t_op <= 60)

rm(df_non_bariatric_for_combined)


#------------------------------------------------------------------------------------------
### Close spark connection to free up memory
#------------------------------------------------------------------------------------------


spark_disconnect(sc) # close the connection
gc()

