# reads in a HES dataset and links on census and deaths

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

hes_dataset_date <- "20240911"



#------------------------------------------------------------------------------------------
# Load in Census
#------------------------------------------------------------------------------------------

# Census 2021 and 2011
cen_2021 <- sdf_sql(sc, "SELECT resident_id, sex, ethnic_group_tb, 
                          is_carer, disability, 
                         ns_sec, highest_qualification, dwelling_address_lsoa,
                         legal_partnership_status, religion_tb, 
                         hh_family_composition, hh_dependent_children, dob_quarter,
                          proxy_answer, linked_to_pds_flag
                         FROM deidentified_cen21_res_attr.deidentified_census_2021_v2")

# remove imputed records
cen_2021 <- cen_2021 %>%
  filter(proxy_answer==1 | proxy_answer==2)

# only include people who link to PDS (so can be linked to HES)
cen_2021 <- cen_2021 %>%
  filter(linked_to_pds_flag=="linked to one nhs number" | linked_to_pds_flag=="linked to multiple nhs numbers") %>%
  select(-linked_to_pds_flag, -proxy_answer)

cen_2021 <- create_dob_21(cen_2021)
cen_2021 <- rename_census21_variables(cen_2021) 

cen_2011 <- sdf_sql(sc,  "SELECT il4_person_id, sex, ethpuk11, carer, disability, 
                          nssec, HLQPUK11,lsoa_code,
                          MARSTAT, RELPUK11, 
                          HHCHUK11, DPCFAMUK11, dob_quarter, present_in_cenmortlink,
                          cen_pr_flag
                          FROM deidentified_census_2011.deidentified_census_2011_v2")

# can link to HES and are not imputed
cen_2011 <- cen_2011 %>%
  filter(present_in_cenmortlink == 1 & cen_pr_flag == 1) %>%
  select(-present_in_cenmortlink, -cen_pr_flag)

cen_2011 <- create_dob_11(cen_2011)
cen_2011 <- rename_census11_variables(cen_2011) 

#create and rename tables for link by census year #######
cen_2011 <- rename(cen_2011, census_id_final = il4_person_id,
                  lsoa_census = lsoa_code) %>%
            select(-c(ethpuk11, nssec, HLQPUK11, MARSTAT, RELPUK11, HHCHUK11, DPCFAMUK11, 
                      carer, disability))
cen_2021 <- rename(cen_2021, census_id_final = resident_id,
                  lsoa_census = dwelling_address_lsoa) %>%
            select(-c(disability, is_carer))

#------------------------------------------------------------------------------------------
# 2.3 Load in Deaths
#------------------------------------------------------------------------------------------

# Deaths data
deaths <- sdf_sql(sc,"SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id
        from deidentified_death_reg_cen_id.deaths_2023_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id
        from deidentified_death_reg_cen_id.deaths_2022_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id
        from deidentified_death_reg_cen_id.deaths_2021_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id
        from deidentified_death_reg_cen_id.deaths_2020_final_v2_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id
        from deidentified_death_reg_cen_id.deaths_2019_v2_std 
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id 
        from deidentified_death_reg_cen_id.deaths_2018_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id 
        from deidentified_death_reg_cen_id.deaths_2017_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id 
        from deidentified_death_reg_cen_id.deaths_2016_v5_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id 
        from deidentified_death_reg_cen_id.deaths_2015_v2_std
        union all
        SELECT doddy, dodmt, dodyr, census2021_person_id, census2011_person_id 
        from deidentified_death_reg_cen_id.deaths_2014_std")

# Create dod
deaths <- deaths %>%
  mutate(dod = paste0(dodyr, dodmt, doddy)) %>%
  mutate(dod = to_date(dod, "yyyy.MM.dd")) %>% 
  #Drop unneeded variables
  select(-c(dodyr, dodmt, doddy))




deaths_2011 <- deaths %>% filter(!is.na(census2011_person_id)) %>%
                          rename(census_id_final = census2011_person_id) %>%
                          select(-c(census2021_person_id))

# if people have two different deaths...take the earliest
deaths_2011 <- deaths_2011 %>%
  group_by(census_id_final) %>%
  filter(dod == min(dod)) %>%
  ungroup() %>%
  sdf_drop_duplicates()

deaths_2021 <- deaths %>% filter(is.na(census2011_person_id) & !is.na(census2021_person_id)) %>%
                          rename(census_id_final = census2021_person_id) %>%
                          select(-c(census2011_person_id))

# if people have two different deaths...take the earliest
deaths_2021 <- deaths_2021 %>%
  group_by(census_id_final) %>%
  filter(dod == min(dod)) %>%
  ungroup() %>%
  sdf_drop_duplicates()



  
#------------------------------------------------------------------------------------------
# 2.3 Load in HES data to link to
#------------------------------------------------------------------------------------------

for (hes_dataset in c("bariatric", "census_non_bariatric")){

if (hes_dataset == "bariatric"){
  analytical_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.bariatric_hes_", hes_dataset_date))
}else if (hes_dataset == "census_non_bariatric"){
  analytical_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.census_non_bariatric_hes_with_opdate_", hes_dataset_date))
}

n_rows <- as.character(sdf_nrow(analytical_df))
n_people <- as.character(sdf_nrow(distinct(analytical_df, census_id_final)))
sample_flow_linked <- data.frame(stage = c("Total number of rows in HES dataset",
                                    "Total number of people in HES dataset"),
                          count = c(n_rows, n_people))
  
  # name of final dataset to save out
table_name <- paste0('hmrc_outcome.', hes_dataset, '_linked_', format(Sys.Date(), '%Y%m%d'))

# name of sample flow file to save out
sample_flow_name <- paste0(outputs_folder,"sample_flow_linked_data_", hes_dataset, "_", format(Sys.Date(), '%Y%m%d'), ".csv")



#------------------------------------------------------------------------------------------
### 2.4 Create linked person level data, census, deaths 
#------------------------------------------------------------------------------------------


# HES dataset with only people with 2011 Census linkage
link_2011 <- analytical_df %>% filter(census_id_flag ==1) 

n_rows <- as.character(sdf_nrow(link_2011))
n_people <- as.character(sdf_nrow(distinct(link_2011, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
                      dplyr::bind_rows(data.frame(stage = c("Total number of rows in HES with Census2011 id",
                                          "Total number of people in HES with Census2011 id"),
                          count = c(n_rows, n_people)))

# link on census 2011 (inner join to get only people who link)
link_2011 <- inner_join(link_2011, cen_2011, by='census_id_final') 

n_rows <- as.character(sdf_nrow(link_2011))
n_people <- as.character(sdf_nrow(distinct(link_2011, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows with link to census 2011",
                                                    "Number of people with link to census 2011"),
                          count = c(n_rows, n_people)))

# link on deaths 2011
link_2011 <- left_join(link_2011, deaths_2011, by='census_id_final') 

n_rows <- as.character(sdf_nrow(link_2011))
n_people <- as.character(sdf_nrow(distinct(link_2011, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows after linking on deaths 2011",
                                                    "Number of people after linking on deaths 2011"),
                          count = c(n_rows, n_people)))


# HES dataset with only people with 2021 Census linkage (without a 2011 linkage)
link_2021 <- analytical_df %>% filter(census_id_flag ==0)

n_rows <- as.character(sdf_nrow(link_2021))
n_people <- as.character(sdf_nrow(distinct(link_2021, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
                      dplyr::bind_rows(data.frame(stage = c("Total number of rows in HES with Census2021 id",
                                          "Total number of people in HES with Census2021 id"),
                          count = c(n_rows, n_people)))

# link on census 2021
link_2021 <- inner_join(link_2021, cen_2021, by='census_id_final') 

n_rows <- as.character(sdf_nrow(link_2021))
n_people <- as.character(sdf_nrow(distinct(link_2021, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows with link to census 2021",
                                                    "Number of people with link to census 2021"),
                          count = c(n_rows, n_people)))


# link on deaths 2011
link_2021 <- left_join(link_2021, deaths_2021, by='census_id_final') 

n_rows <- as.character(sdf_nrow(link_2021))
n_people <- as.character(sdf_nrow(distinct(link_2021, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows after linking on deaths",
                                                    "Number of people after linking on deaths"),
                          count = c(n_rows, n_people)))



# combine 2011 and 2021
linked_data_nhs_cen <- rbind(link_2011, link_2021)  

n_rows <- as.character(sdf_nrow(linked_data_nhs_cen))
n_people <- as.character(sdf_nrow(distinct(linked_data_nhs_cen, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
                      dplyr::bind_rows(data.frame(stage = c("Total number of rows in linked HES, deaths, Census",
                                    "Total number of people in linked HES, deaths, Census"),
                          count = c(n_rows, n_people)))

                                                              
#------------------------------------------------------------------------------------------
# 2.1 Load in Load IMD, rural urban and region for linkage
#------------------------------------------------------------------------------------------

# where the census lsoa is from the 2021 Census, convert to 2011 lsoa
lsoa_2021_to_2011 <- copy_to(sc, read.csv('<filepath>/lsoa2011_to_lsoa2021_lookup.csv'), overwrite = TRUE)
lsoa_2021_to_2011 <- select(lsoa_2021_to_2011, LSOA11CD, LSOA21CD)

linked_data_nhs_cen <- left_join(linked_data_nhs_cen, lsoa_2021_to_2011, 
                                 by=c('lsoa_census'='LSOA11CD'))

linked_data_nhs_cen <- linked_data_nhs_cen %>%
    mutate(lsoa_census = ifelse(census_id_flag==0, LSOA21CD, lsoa_census)) %>%
    select(-LSOA21CD)

if (!("lsoa11" %in% colnames(linked_data_nhs_cen))){
  linked_data_nhs_cen <- linked_data_nhs_cen %>%
    mutate(lsoa_final = lsoa_census)
}else{
  # where there is no lsoa from HES, use census lsoa
  linked_data_nhs_cen <- linked_data_nhs_cen %>%
    mutate(lsoa_final = ifelse(!is.na(lsoa11), lsoa11, lsoa_census))
}

# filter to people who live in England
linked_data_nhs_cen <- linked_data_nhs_cen %>%
  filter(substring(lsoa_final, 1, 1) == "E")

n_rows <- as.character(sdf_nrow(linked_data_nhs_cen))
n_people <- as.character(sdf_nrow(distinct(linked_data_nhs_cen, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows with address in England",
                                                    "Number of people with address in England"),
                          count = c(n_rows, n_people)))

## Load IMD and rural urban classification and link to data 

IMD <-spark_read_csv(sc,name = "IMD_2019", path = "<filepath>/IMD_2019.csv", header = TRUE, delimiter = ",")
rural_urban <- copy_to(sc, read.csv('<filepath>/Rural_urban_classification_2011.csv'), overwrite = TRUE)
region <- copy_to(sc, read.csv('<filepath>/lsoa2011_to_region_2020_lookup.csv'), overwrite = TRUE)

# Rename IMD variables
IMD <- IMD %>%
        rename(lsoa11cd = LSOA11CD,
              imd_decile = IMD_DECILE,
              imd_quintile = IMD_QUINTILE) %>%
        select(lsoa11cd, imd_decile, imd_quintile)

rural_urban <- rural_urban %>%
              rename(lsoa11cd = LSOA11CD,
                    ruc11cd = RUC11CD,
                    ruc11= RUC11) %>%
              select(lsoa11cd, ruc11cd, ruc11) %>%
              mutate(rural_urban = case_when(grepl('Urban', ruc11) ~ 'Urban',
                                             grepl('Rural', ruc11) ~ 'Rural',
                                             TRUE ~ 'Error' ))


region <- region %>%
  rename(lsoa11cd = LSOA11CD,
        region = RGN20NM) %>%
  select(lsoa11cd, region)



#Link to main data
linked_data_nhs_cen <- linked_data_nhs_cen %>%
            left_join(IMD, by = c('lsoa_final' = 'lsoa11cd'))

linked_data_nhs_cen <- linked_data_nhs_cen %>%
                    left_join(rural_urban, by = c('lsoa_final' = 'lsoa11cd'))

linked_data_nhs_cen <- linked_data_nhs_cen %>%
                    left_join(region, by = c('lsoa_final' = 'lsoa11cd'))


#------------------------------------------------------------------------------------------
# 2.1 Filtering on age
#------------------------------------------------------------------------------------------

linked_data_nhs_cen <- linked_data_nhs_cen %>% 
                            #calculate age at operation date and select based on 21-64 criteria
                            mutate(op_year = as.numeric(substring(op_date,1,4)),
                            dob_year = as.numeric(substring(prov_dob,1,4))) %>%
                            mutate(age_at_operation = ifelse(substring(op_date,6,10) >= substring(prov_dob,6,10), 
                                                            op_year - dob_year, (op_year - dob_year) -1)) %>%
                            select(-c(op_year)) 

# get the age on the last day of hte month in which the operation occurred
linked_data_nhs_cen <- linked_data_nhs_cen %>% 
  mutate(last_day_op_month = last_day(op_date)) %>%
  mutate(last_day_op_month_year = as.numeric(substring(last_day_op_month,1,4))) %>%
  mutate(age_on_last_day_op_month = ifelse(substring(last_day_op_month,6,10) >= substring(prov_dob,6,10), 
                                        last_day_op_month_year - dob_year, (last_day_op_month_year - dob_year) -1)) %>%
                            select(-c(dob_year, last_day_op_month_year)) 

# filter to working age on last day of month of operation (so does not turn 65 in month of operation)
# and working age on date of operation
linked_data_nhs_cen <- linked_data_nhs_cen %>% 
                            mutate(working_age_flag_last_day = case_when(age_on_last_day_op_month >= 25 & age_on_last_day_op_month <= 64 ~ 1,
                                                  TRUE ~ 0)) %>%
                            mutate(working_age_flag_op_date = case_when(age_at_operation >= 25 & age_at_operation <= 64 ~ 1,
                                                  TRUE ~ 0)) %>%
                            filter(working_age_flag_last_day == 1 & working_age_flag_op_date == 1) %>%
                            select(-working_age_flag_last_day, -working_age_flag_op_date)

n_rows <- as.character(sdf_nrow(linked_data_nhs_cen))
n_people <- as.character(sdf_nrow(distinct(linked_data_nhs_cen, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows age 25 to 64 on operation day and last day of month of operation",
                                                    "Number of people age 25 to 64 on operation day and last day of month of operation"),
                          count = c(n_rows, n_people)))



## remove people who died during or before month of index date
linked_data_nhs_cen <- linked_data_nhs_cen %>%                          
                         filter(is.na(dod) | dod > last_day(op_date))

n_rows <- as.character(sdf_nrow(linked_data_nhs_cen))
n_people <- as.character(sdf_nrow(distinct(linked_data_nhs_cen, census_id_final)))
sample_flow_linked <- sample_flow_linked %>%
               dplyr::bind_rows(data.frame(stage = c("Number of rows for people who did not die before index date",
                                                    "Number of people who did not die before index date"),
                          count = c(n_rows, n_people)))

write.csv(sample_flow_linked, sample_flow_name, row.names=FALSE)

#------------------------------------------------------------------------------------------
### SAVE OUT FINAL LINKED DATASET
#------------------------------------------------------------------------------------------ 

tbl_change_db(sc, "hmrc_outcome")

sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(linked_data_nhs_cen , 'linked_data_nhs_cen')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM linked_data_nhs_cen')
DBI::dbExecute(sc, sql)

}



