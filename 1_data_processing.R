 ######## data processing #########

 #import librarys
library(dplyr)
library(sparklyr)
library(lubridate)
library(stringr)
options(sparkly.dplyr_distinct.impl = 'tbl_lazy')

#set up spark connection 
config <- spark_config()
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.scores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
config$spark.yarn.executor.memoryOverhead <- "2g"
config$spark.executor.cores <- 5
config$spark.sql.shuffle.partitions <- 240

sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")

#########################################################################################################################
#         1- CREATE FLAGGED PROCESSING DATASET                                                                          # 
#         INPUTS                                                                                                        # 
#         bariatric_icd_lookup.csv - diagnosis codes for obesity (ICD-10)                                               #
#         bariatric_opcs_lookup.csv - procecure codes for bariatric surgeries (OPCS) excluding revision surgeries       #
#         bariatric_associated_cond_lookup.csv - conditions associated with bariatric surgery and obesity               #
#                                                                                                                       #
#         pre_2017_apc - all HES APC data flagged for bariatric procedures between 2009 and 2016 at episode level       #  
#         post_2017_apc - all HES APC data flagged for bariatric procedures between 2017 and 2022 at episode level      # 
#                                                                                                                       #
#         OUTPUTS                                                                                                       # 
#         previous_surgeries_tbl - list of nhs numbers for eligible people who had bariatric surgery prior to 2017      # 
#         processing_df - all bariatric procedure episodes post 2017 (flagged) without study exclusions                 #
#                                                                                                                       #
#########################################################################################################################

#provide dataset name for episodic HES APC data years 2009 - 2016
pre_2017_apc <- "pre2017_apc_data"

#provide dataset name for episodic HES APC data years 2017 - 2022
post_2017_apc <- "pre2017_apc_data"

#set path to git repo
path_dir <- 'path_to_directory'
bariatric_procedure_codes <- read.csv(paste0(path_dir, '/bariatric_icd_lookup.csv'))
bariatric_diag_codes <- read.csv(paste0(path_dir, '/bariatric_opcs_lookup.csv'))
associated_cond_codes <- read.csv(paste0(path_dir, '/bariatric_associated_cond_lookup.csv'))

procedure_codes <- bariatric_procedure_codes[[2]]
diag_codes <- bariatric_diag_codes[[2]]
associated_codes <- associated_cond_codes[[2]]

#use pre 2017 APC episodes to identify who has had previous bariatric surgery for exclusion
previous_surgeries <- sdf_sql(sc, "SELECT DISTINCT newnhsno_hes, opertn_4_01, 
                              opertn_4_02 FROM pre_2017_apc where newnhsno_hes in (select newnhsno_hes from post_2017_apc)")

previous_surgeries_tbl <- previous_surgeries %>% filter(opertn_4_01 %in% procedure_codes | opertn_4_02 %in% procedure_codes) %>%
                          distinct(newnhsno_hes) %>%
                          select(newnhsno_hes) %>% collect()

#save the previous surgery records in case we need to refer back
write.csv(paste0(previous_surgeries_tbl, '/previous_surgery_records.csv'))

#load in APC episode data (2017 - 2022 years combined)           
df <- sdf_sql(sc, paste0("SELECT * FROM post_2017_apc"))

#create flagged processing dataset 
#flag for those with primary and secondary diagnosis of obesity according to NOA guidelines (primary_obesity_flag / secondary_obesity_flag)
#flag those of working age according to ONS study 21 - 64 yrs
#flag for anyone with previous metabolic surgery (pre 2017)
#flag for anyone with metabolic operation code as primary or secondary operation 
#flag for primary diagnosis in associated conditions list 

processing_df <- df %>% mutate(primary_obesity_flag = case_when(diag_4_01 %in% diag_codes ~ 1,
                                                                TRUE ~ 0),
                               working_age_flag = case_when(admiage_hes >= 21 & admiage_hes <= 64 ~ 1,
                                                            TRUE ~ 0),
                               prev_metab_surgery_flag = case_when(newnhsno_hes %in% local(previous_surgeries_tbl$newnhsno_hes) ~ 1,
                                                                   TRUE ~ 0),
                               metabolic_op_flag = case_when(opertn_4_01 %in% procedure_codes | opertn_4_02 %in% procedure_codes ~ 1,
                                                             TRUE ~ 0),
                               secondary_obesity_flag = case_when(diag_4_02 %in% diag_codes ~ 1,
                                                                  TRUE ~ 0),
                               associated_cond_flag = case_when(diag_4_01 %in% associated_codes ~ 1,
                                                                TRUE ~ 0))
#save as hive table 
table_name <- "name_of_table"
table_path <- "path_of_table"

table_name = paste0(table_path.table_name)
tbl_change_db(sc, table_path)

sql <- paste0('DROP TABLE IF EXISTS', table_name)
DBI::dbExecute(sc, sql)

sdf_register(processing_df, 'processing_df')
sql <- paste0('CREATE TABLE', table_name, 'AS SELECT * FROM processing_df')
DBI::dbExecute(sc, sql)

#########################################################################################################################
#         2- CREATE FLAGGED ANALYTICAL DATASET                                                                          # 
#         INPUTS                                                                                                        # 
#         processing_df - sql data table containing post 2017 apc episodes with flags                                   #
#                                                                                                                       #
#         OUTPUTS                                                                                                       #               
#         analytical_df - post 2017 APC data filtered for study exclusions                                              # 
#                                                                                                                       #
#########################################################################################################################

#use processing df flags to filter according to ONS study exclusion criteria 
analytical_df <- processing_df %>% filter(prev_metab_surgery_flag == 0 & metabolic_op_flag ==1) %>%
                                   filter(primary_obesity_flag == 1 & working_age_flag == 1 | secondary_obesity_flag == 1 & associated_cond_flag == 1 & working_age_flag == 1)

#arrange by episode start date and select only the earliest to get index 
analytical_df <- analytical_df %>% collect() %>% group_by(newnhsno_hes) %>% arrange(epistart_hes) %>% slice(1L)

table_name <- "name_of_table"
table_path <- "path_of_table"

table_name = paste0(table_path.table_name)
tbl_change_db(sc, table_path)

sql <- paste0('DROP TABLE IF EXISTS', table_name)
DBI::dbExecute(sc, sql)

sdf_register(processing_df, 'analytical_df')
sql <- paste0('CREATE TABLE', table_name, 'AS SELECT * FROM analytical_df')
DBI::dbExecute(sc, sql)




