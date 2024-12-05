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
library(stddiff)
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
source("<filepath>/modelling_functions.R")
source("<filepath>/config.R")


dataset_date <- "20240911"

HES_date <- as.Date("20240911", "%Y%m%d")
linked_date <- "20240911"
sample_date <- as.Date("20240911", "%Y%m%d")

outputs_folder <- <filepath>
descriptives_folder <- paste0(outputs_folder, "descriptives/")

##########################################################################################
##                                Person level descriptives                             ##
##########################################################################################

for (dataset in c("bariatric", "census_non_bariatric")){
  
dataset_folder <- paste0(dataset, "_", dataset_date)

save_folder <- paste0(descriptives_folder, dataset_folder, "/")

dir.create(file.path(descriptives_folder, dataset_folder))
  
#------------------------------------------------------------------------------------------
### Summary and descriptive statistics inc. graphs for personal level data
#-----------------------------------------------------------------------------------------

if (dataset == "bariatric"){
  linked_data <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.bariatric_linked_monthly_hmrc_", dataset_date))
}else if (dataset == "census_non_bariatric"){
  linked_data <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.census_non_bariatric_linked_monthly_hmrc_", dataset_date))
}

linked_data <- add_deflated_pay(linked_data, pay)

linked_data <- linked_data %>%
  mutate(in_work = ifelse(pay > 0, 1, 0))

linked_data_person_level <- linked_data %>% filter(t_op == 1)


#------------------------------------------------------------------------------------------
# Sample flow
#------------------------------------------------------------------------------------------

# read in csvs
sf_HES <- read.csv(paste0(outputs_folder, <filepath>))
sf_linked <- read.csv(paste0(outputs_folder, <filepath>))
sf_monthly <- read.csv(paste0(outputs_folder, <filepath>))

if (dataset != "bariatric"){
  sf_sample <- read.csv(paste0(outputs_folder, "dataset_construction/Sampling_sample_flow_", dataset, "_", sample_date, ".csv"))
  # combine
  sf <- bind_rows(sf_HES, sf_linked, sf_sample, sf_monthly)
}else{sf_HES$count <- as.character(sf_HES$count)
      sf_linked$count <- as.character(sf_linked$count)
      sf_monthly$count <- as.character(sf_monthly$count)
      sf <- bind_rows(sf_HES, sf_linked, sf_monthly)
     }


#------------------------------------------------------------------------------------------
# Age descriptives
#------------------------------------------------------------------------------------------

# Distribution of ages 
age_distribution_all <- linked_data_person_level %>%
  group_by(age_at_operation) %>%
  summarise(Count = n()) %>%
  arrange(age_at_operation) %>%
  collect()

# Age stats - min, max, mean
age_stats_all <- linked_data_person_level %>%
  summarise(
    min_age = min(age_at_operation),
    max_age = max(age_at_operation),
    mean_age = mean(age_at_operation),
    sd_age = sd(age_at_operation)
  ) %>% collect()


# Distribution of age chart    
ggplot(age_distribution_all, aes(x=age_at_operation, y=Count)) + 
  geom_bar(stat="identity", fill = "lightblue") +
  labs(title="Distribution of age at person level",
       x ="Age", 
       y = "Frequency") +
  scale_x_continuous(breaks=seq(10,80, by = 5)) +
  geom_vline(xintercept = age_stats_all$mean_age, col='black', linetype = 'dashed', size=1)+
  annotate(x=age_stats_all$mean_age,y=+Inf,label="Mean age",vjust=2,geom="label") +
  theme_bw()

ggsave(file= paste0(save_folder,"age_person_level.png"), width = 10, height = 4, units = "in")
                  

#------------------------------------------------------------------------------------------
# Pay and employment by age and sex
#------------------------------------------------------------------------------------------

pay_and_employment_by_age <- linked_data %>%
  group_by(age_monthly, census_sex) %>%
  summarise(average_pay = mean(pay),
           average_pay_deflated = mean(pay_deflated),
           prop_in_work = sum(in_work)/n(),
           n_people = n(),
           test = mean(pay_deflated, na.rm=FALSE)) %>%
  collect()


pay_in_work_by_age <- linked_data %>%
  filter(in_work == 1) %>%
  group_by(age_monthly, census_sex) %>%
  summarise(average_pay_in_work = mean(pay),
           average_pay_deflated_in_work = mean(pay_deflated)) %>%
  collect()

pay_and_employment_by_age <- left_join(pay_and_employment_by_age, pay_in_work_by_age, 
                                       by=c("age_monthly", "census_sex"))

pay_and_employment_by_age <- arrange(pay_and_employment_by_age, age_monthly)
  
ggplot(pay_and_employment_by_age, aes(x = age_monthly, y = average_pay, colour = census_sex)) +
  geom_line(alpha=0.6)  + 
  labs(title="Pay by age",
        x ="Age (monthly)", 
        y = "Average monthly pay per person") +
  theme_bw() +
  xlim(c(20, 70))

ggsave(file= paste0(save_folder,"average_monthly_pay_by_monthly_age.png"), width = 10, height = 4, units = "in")

ggplot(pay_and_employment_by_age, aes(x = age_monthly, y = average_pay_in_work, colour = census_sex)) +
  geom_line(alpha=0.6)  + 
  labs(title="Pay by age",
        x ="Age (monthly)", 
        y = "Average monthly pay (in work) per person") +
  theme_bw() +
  xlim(c(20, 70))

ggsave(file= paste0(save_folder,"average_monthly_pay_in_work_by_monthly_age.png"), width = 10, height = 4, units = "in")

ggplot(pay_and_employment_by_age, aes(x = age_monthly, y = prop_in_work, colour = census_sex)) +
  geom_line(alpha=0.6)  + 
  labs(title="Employment by age",
        x ="Age (monthly)", 
        y = "Proportion employed") +
  theme_bw() +
  xlim(c(20, 70))

ggsave(file= paste0(save_folder,"proportion_employed_by_monthly_age.png"), width = 10, height = 4, units = "in")


#------------------------------------------------------------------------------------------
# Counts
#------------------------------------------------------------------------------------------

#List of variables for counts for loop/function (calculate_var_count function)
argument_list <- c('census_sex', 
                   'age_bands',
                   'ethnic_group_tb',
                   'ns_sec',
                   'disability_category',
                   'disability_binary',
                   'rural_urban',
                   'imd_decile',
                   'imd_quintile',
                  'region',
                  'religion_tb',
                  'highest_qualification',
                  'legal_partnership_status')

# Calculating counts for categorical variables for all people
results_cat_list <- purrr::map(argument_list, calculate_var_count)

# Bind the results into a single DataFrame
categorical_counts_all_result <- do.call(sdf_bind_rows, results_cat_list) %>% collect()

# add mean age
age <- data.frame(Variable_names = "age_at_operation", Category_name = "",
                 Count = age_stats_all$mean_age,
                 Percentage = age_stats_all$sd_age)

categorical_counts_all_result <- bind_rows(categorical_counts_all_result, age)

#------------------------------------------------------------------------------------------
# Standardised differences
#------------------------------------------------------------------------------------------

# do for non exposed datasets

if (dataset != "bariatric"){
  
  # read in bariatric data for comparison
  bariatric_df <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.bariatric_linked_monthly_hmrc_", dataset_date))
  exposed_df <- bariatric_df %>% filter(t_op == 1) 

  # add group column
  exposed_df <- exposed_df %>%
    mutate(group = 0) %>%
    select(group, census_sex, disability_binary, disability_category, ethnic_group_tb,
        age_bands, ns_sec, rural_urban, imd_decile, age_at_operation, region,
          religion_tb, highest_qualification, legal_partnership_status)
  non_exposed_df <- linked_data_person_level %>%
    mutate(group = 1) %>%
    select(group, census_sex, disability_binary, disability_category, ethnic_group_tb,
        age_bands, ns_sec, rural_urban, imd_decile, age_at_operation, region,
          religion_tb, highest_qualification, legal_partnership_status)

  # combine and collect
  linked_data_combined <- sdf_bind_rows(exposed_df, non_exposed_df)
  linked_data_combined <- collect(linked_data_combined)
  linked_data_combined <- data.frame(linked_data_combined)

  
  cols_category <- c("census_sex", "disability_category", "ethnic_group_tb",
        "age_bands", "ns_sec", "rural_urban", "imd_decile", "disability_binary",
                    "religion_tb", "region", "highest_qualification",
                    "legal_partnership_status")
  
  cols_numeric <- c("age_at_operation")
  

  std_diff_all <- std_diffs(linked_data_combined, 
            cols_category=cols_category, 
            cols_numeric=cols_numeric, 
            cols_binary=cols_binary)

  # add to descriptives table
  categorical_counts_all_result <- full_join(categorical_counts_all_result, std_diff_all, 
                                               by=c("Variable_names" = "variable"))

}

data.frame(categorical_counts_all_result)

#------------------------------------------------------------------------------------------
## Summary graphs and tables for operations
#------------------------------------------------------------------------------------------

if (dataset == "bariatric"){

  # totals of operations (all and by type)
op_count <- linked_data_person_level %>% 
  group_by(operation_type) %>%
  summarise(Count = n()) %>%
  arrange(-Count) %>%
  collect()

ggplot(op_count, aes(x=reorder(operation_type, -Count), y=Count)) + 
  geom_bar(stat="identity", fill = "lightblue") +
  labs(title="Counts of operation types for index operation",
       x ="Operation type", 
       y = "Count") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggsave(file= paste0(save_folder,"operation_count_by_type.png"), width = 10, height = 4, units = "in")
         

# totals by code
op_count_by_code <- linked_data_person_level %>% 
  group_by(op_code) %>%
  summarise(Count = n()) %>%
  arrange(-Count) %>%
  collect() 

linked_data_person_level <- linked_data_person_level %>% 
  mutate(cal_month = months_between(ref_date, "2014-03-31") - 1)
  
op_count_month <- linked_data_person_level %>%
  mutate(month = substr(ref_date, 1, 7)) %>%
  group_by(cal_month, month, operation_type) %>%
  count() %>%
  collect() %>%
  arrange(cal_month)
  
op_count_month_total <- linked_data_person_level %>%
  mutate(month = substr(ref_date, 1, 7)) %>%
  group_by(cal_month, month) %>%
  count() %>%
  collect() %>%
  mutate(operation_type = "total")

op_count_month <- bind_rows(op_count_month, op_count_month_total)
  
ggplot(op_count_month, aes(x=cal_month, y=n, colour = operation_type)) +
  geom_line() + 
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 600, by = 12)) +
  labs(title="Number of operations by month of operation",
        x ="Date", y = "Number of operations")
  
ggsave(paste0(save_folder,"operations_count_by_type_and_month.png"), width = 14, height = 4, units = "in")


}


#------------------------------------------------------------------------------------------
## Overall average pay and probability of employment for datasets
#------------------------------------------------------------------------------------------
  
pay_and_employment_overall <- linked_data %>% 
  summarise(average_monthly_pay = mean(pay,na.rm = TRUE),
           median_monthly_pay = percentile(pay, 0.5),
          average_monthly_pay_deflated = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated = percentile(pay_deflated, 0.5),
            max_monthly_pay = round(max(pay), -2),
            max_monthly_pay_deflated = round(max(pay_deflated), -2),
           prop_in_work = sum(in_work)/n()) %>%
  collect()

pay_and_employment_overall_in_work <- linked_data %>% 
  filter(in_work == 1) %>%
  summarise(average_monthly_pay_in_work = mean(pay,na.rm = TRUE),
           median_monthly_pay_in_work = percentile(pay, 0.5),
          average_monthly_pay_deflated_in_work = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated_in_work = percentile(pay_deflated, 0.5),
            max_monthly_pay_in_work = round(max(pay), -2),
            max_monthly_pay_deflated_in_work = round(max(pay_deflated), -2)) %>%
  collect()


pay_and_employment_overall <- bind_cols(pay_and_employment_overall,
                                          pay_and_employment_overall_in_work)

pay_and_employment_overall$group = "overall"

pay_and_employment_pre_post_surgery <- linked_data %>% 
  mutate(group = ifelse(t_op<0, "pre-surgery", "post-surgery")) %>%
  group_by(group) %>%
  summarise(average_monthly_pay = mean(pay,na.rm = TRUE),
           median_monthly_pay = percentile(pay, 0.5),
          average_monthly_pay_deflated = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated = percentile(pay_deflated, 0.5),
            max_monthly_pay = round(max(pay), -2),
            max_monthly_pay_deflated = round(max(pay_deflated), -2),
            prop_in_work = sum(in_work)/n()) %>%
  collect()

pay_and_employment_pre_post_surgery_in_work <- linked_data %>% 
  filter(in_work == 1) %>%
  mutate(group = ifelse(t_op<0, "pre-surgery", "post-surgery")) %>%
  group_by(group) %>%
  summarise(average_monthly_pay_in_work = mean(pay,na.rm = TRUE),
           median_monthly_pay_in_work = percentile(pay, 0.5),
          average_monthly_pay_deflated_in_work = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated_in_work = percentile(pay_deflated, 0.5),
             max_monthly_pay_in_work = round(max(pay), -2),
            max_monthly_pay_deflated_in_work = round(max(pay_deflated), -2)) %>%
  collect()

pay_and_employment_pre_post_surgery <- left_join(pay_and_employment_pre_post_surgery,
                                          pay_and_employment_pre_post_surgery_in_work,
                                                by = "group")

pay_and_employment_summary <- bind_rows(pay_and_employment_overall,
                                          pay_and_employment_pre_post_surgery)


#------------------------------------------------------------------------------------------
## Summary graphs for pay and employment by event time
#------------------------------------------------------------------------------------------

pay_by_time_to_op <- linked_data %>% 
  group_by(t_op) %>% 
  summarise(average_monthly_pay = mean(pay,na.rm = TRUE),
           median_monthly_pay = percentile(pay, 0.5),
          average_monthly_pay_deflated = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated = percentile(pay_deflated, 0.5),
            prop_in_work = sum(in_work)/n(),
           n_people = n()) %>%
  collect()

pay_by_time_to_op_in_work <- linked_data %>% 
  filter(pay > 0) %>%
  group_by(t_op) %>% 
  summarise(average_monthly_pay_in_work = mean(pay,na.rm = TRUE),
           median_monthly_pay_in_work = percentile(pay, 0.5),
          average_monthly_pay_deflated_in_work = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated_in_work = percentile(pay_deflated, 0.5)) %>%
  collect()


pay_by_time_to_op <- left_join(pay_by_time_to_op, pay_by_time_to_op_in_work, by="t_op")
pay_by_time_to_op <- arrange(pay_by_time_to_op, t_op) 

ggplot(pay_by_time_to_op, aes(x = t_op, y = average_monthly_pay)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay by time to operation",
        x ="Time to operation in months", 
        y = "Average monthly pay per person") +
  geom_vline(xintercept = 0, linetype="dashed", 
                color = "black") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 60, by = 12))

ggsave(file= paste0(save_folder,"average_monthly_pay_by_time_to_op.png"), width = 10, height = 4, units = "in")


ggplot(pay_by_time_to_op, aes(x = t_op, y = average_monthly_pay_deflated)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay (deflated to 2023) by time to operation",
        x ="Time to operation in months", 
        y = "Average monthly pay (deflated to 2023) per person") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
     legend.position="bottom") + 
  labs(color='Legend:') +
  geom_vline(xintercept = 0, linetype="dashed", 
                color = "black") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 60, by = 12))

ggsave(file= paste0(save_folder,"average_monthly_pay_deflated_by_time_to_op.png"), width = 10, height = 4, units = "in")


ggplot(pay_by_time_to_op, aes(x = t_op, y = prop_in_work*100)) +
  geom_line(alpha=0.6)  + 
  labs(x ="Time to operation in months", 
        y = "Proportion of individuals in work") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
     legend.position="bottom") + 
  labs(color='Legend:') +
  geom_vline(xintercept = 0, linetype="dashed", 
                color = "black") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 60, by = 12))

ggsave(file= paste0(save_folder,"proportion_in_work.png"), width = 8, height = 6, units = "in")


ggplot(pay_by_time_to_op, aes(x = t_op, y = n_people)) +
  geom_line(alpha=0.6)  + 
  labs(x ="Time to operation in months", 
        y = "Total number of individuals") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
     legend.position="bottom") + 
  labs(color='Legend:') +
  geom_vline(xintercept = 0, linetype="dashed", 
                color = "black") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 60, by = 12))

ggsave(file= paste0(save_folder,"n_people_by_month_to_op.png"), width = 8, height = 6, units = "in")



if (dataset == "bariatric"){
  
  # histogram of all pay data (main analysis)
  all_pay <- linked_data %>%
    select(t_op, pay) %>%
    collect()

  ggplot(all_pay, aes(x=pay)) + 
    geom_histogram(binwidth=100) +
    xlim(xmin=0, xmax=10000) +
    theme_bw() + 
    geom_histogram(color="black", fill="white")

  ggsave(file= paste0(save_folder,"monthly_pay_hist.png"), width = 8, height = 6, units = "in")
}



#------------------------------------------------------------------------------------------
## Calendar time trends in pay and employment
#------------------------------------------------------------------------------------------

linked_data <- linked_data %>% 
  mutate(cal_month = months_between(ref_date, "2014-03-31") - 1)

pay_deflated_calendar <- linked_data %>% 
  group_by(cal_month) %>% 
  summarise(average_monthly_pay_deflated = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated = percentile(pay_deflated, 0.5),
            average_monthly_pay = mean(pay,na.rm = TRUE),
           median_monthly_pay = percentile(pay, 0.5),
           prop_in_work = sum(in_work)/n(),
           n_people = n()) %>%
  collect()


pay_deflated_in_work_calendar <- linked_data %>% 
  filter(pay > 0) %>%
  group_by(cal_month) %>% 
  summarise(average_monthly_pay_deflated_in_work = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated_in_work = percentile(pay_deflated, 0.5),
            average_monthly_pay_in_work = mean(pay,na.rm = TRUE),
           median_monthly_pay_in_work = percentile(pay, 0.5)) %>%
  collect()

pay_by_cal_month <- left_join(pay_deflated_calendar, pay_deflated_in_work_calendar, by="cal_month")


if (dataset == "bariatric"){
  
  # censor at operation for bariatric_dataset
  linked_data_censor_at_op <- linked_data %>%
    filter(t_op <0)

  pay_deflated_calendar_pre_op <- linked_data_censor_at_op %>% 
    group_by(cal_month) %>% 
    summarise(average_monthly_pay_deflated = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated = percentile(pay_deflated, 0.5),
            average_monthly_pay = mean(pay,na.rm = TRUE),
           median_monthly_pay = percentile(pay, 0.5),
            prop_in_work = sum(in_work)/n(),
           n_people = n()) %>%
  collect()


  pay_deflated_in_work_calendar_pre_op <- linked_data_censor_at_op %>% 
    filter(pay > 0) %>%
    group_by(cal_month) %>% 
    summarise(average_monthly_pay_deflated_in_work = mean(pay_deflated,na.rm = TRUE),
           median_monthly_pay_deflated_in_work = percentile(pay_deflated, 0.5),
            average_monthly_pay_in_work = mean(pay,na.rm = TRUE),
           median_monthly_pay_in_work = percentile(pay, 0.5)) %>%
  collect()
  
  pay_by_cal_month_pre_op <- left_join(pay_deflated_calendar_pre_op, pay_deflated_in_work_calendar_pre_op, by="cal_month")
  pay_by_cal_month_pre_op$series <- "pre_op"
  pay_by_cal_month$series <- "all"
  
  # join on pre op rows
  pay_by_cal_month <- bind_rows(pay_by_cal_month, pay_by_cal_month_pre_op)
  
  ggplot(pay_by_cal_month,
         aes(x = cal_month, y = median_monthly_pay_deflated,colour = series)) +
    geom_line(alpha=0.6)  + 
    labs(title="Monthly pay (deflated to 2023) by calendar month (all)",
        x ="Calendar month", 
        y = "Median monthly pay (deflated to 2023) per person") +
    theme_bw() +
    scale_x_continuous(breaks = seq(-60, 600, by = 12))

  ggsave(file= paste0(save_folder,"median_monthly_pay_deflated_by_calendar_month_with_pre_op.png"), width = 10, height = 4, units = "in")
  
  ggplot(pay_by_cal_month, 
         aes(x = cal_month, y = prop_in_work*100, colour = series)) +
  geom_line()  + 
  labs(x ="Calendar month", 
        y = "Proportion of individuals in work") +
  theme_bw() +
  scale_x_continuous(breaks = seq(0, 108, by = 12), 
                     labels = c("April 2014", "April 2015", "April 2016", "April 2017",
                               "April 2018", "April 2019", "April 2020", "April 2021",
                               "April 2022", "April 2023"))
  
  pay_by_cal_month_to_plot <- filter(pay_by_cal_month, series == "all")
         
         
}else{pay_by_cal_month_to_plot <- pay_by_cal_month}


pay_by_cal_month_to_plot <- arrange(pay_by_cal_month_to_plot, cal_month)


ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = median_monthly_pay_deflated)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay (deflated to 2023) by calendar month (all)",
        x ="Calendar month", 
        y = "Median monthly pay (deflated to 2023) per person") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 600, by = 12))

ggsave(file= paste0(save_folder,"median_monthly_pay_deflated_by_calendar_month.png"), width = 10, height = 4, units = "in")

ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = median_monthly_pay_deflated_in_work)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay (deflated to 2023) by calendar month (in work only)",
        x ="Calendar month", 
        y = "Median monthly pay (deflated to 2023) per person") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 600, by = 12))

ggsave(file= paste0(save_folder,"median_monthly_pay_deflated_by_calendar_month_in_work.png"), width = 10, height = 4, units = "in")

ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = average_monthly_pay_deflated)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay (deflated to 2023) by calendar month (all)",
        x ="Calendar month", 
        y = "Average monthly pay (deflated to 2023) per person") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 600, by = 12))

ggsave(file= paste0(save_folder,"average_monthly_pay_deflated_by_calendar_month.png"), width = 10, height = 4, units = "in")

ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = average_monthly_pay_deflated_in_work)) +
  geom_line(alpha=0.6)  + 
  labs(title="Monthly pay (deflated to 2023) by calendar month (in work only)",
        x ="Calendar month", 
        y = "Average monthly pay (deflated to 2023) per person") +
  theme_bw() +
  scale_x_continuous(breaks = seq(-60, 600, by = 12))

ggsave(file= paste0(save_folder,"average_monthly_pay_deflated_by_calendar_month_in_work.png"), width = 10, height = 4, units = "in")


ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = prop_in_work*100)) +
  geom_line()  + 
  labs(x ="Calendar month", 
        y = "Proportion of individuals in work") +
  theme_bw() +
  scale_x_continuous(breaks = seq(0, 108, by = 12), 
                     labels = c("April 2014", "April 2015", "April 2016", "April 2017",
                               "April 2018", "April 2019", "April 2020", "April 2021",
                               "April 2022", "April 2023"))

ggsave(file= paste0(save_folder,"proportion_in_work_by_cal_month.png"), width = 8, height = 6, units = "in")


ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = prop_in_work*100)) +
  geom_line()  + 
  labs(x ="Calendar month", 
        y = "Proportion of individuals in work pre operation only") +
  theme_bw() +
  scale_x_continuous(breaks = seq(0, 108, by = 12), 
                     labels = c("April 2014", "April 2015", "April 2016", "April 2017",
                               "April 2018", "April 2019", "April 2020", "April 2021",
                               "April 2022", "April 2023"))


ggsave(file= paste0(save_folder,"proportion_in_work_by_cal_month_pre_op.png"), width = 8, height = 6, units = "in")


ggplot(pay_by_cal_month_to_plot, aes(x = cal_month, y = n_people)) +
  geom_line(alpha=0.6)  + 
  labs(x ="Calendar month", 
        y = "Total number of individuals") +
  theme_bw() +
  scale_x_continuous(breaks = seq(0, 108, by = 12), 
                     labels = c("April 2014", "April 2015", "April 2016", "April 2017",
                               "April 2018", "April 2019", "April 2020", "April 2021",
                               "April 2022", "April 2023"))


ggsave(file= paste0(save_folder,"n_people_by_cal_month.png"), width = 8, height = 6, units = "in")



#------------------------------------------------------------------------------------------
## Follow up time
#------------------------------------------------------------------------------------------

# average follow up length by operation month
follow_up <- linked_data %>% 
  group_by(census_id_final) %>% 
  mutate(follow_up = max(t_op) - min(t_op) + 1) %>%
  ungroup() %>%
  select(census_id_final, follow_up) %>%
  distinct() %>%
  summarise(average_follow_up = mean(follow_up),
            median_follow_up = percentile(follow_up, 0.5),
           max_follow_up = max(follow_up),
           min_follow_up = min(follow_up)) %>%
  mutate(group = "overall") %>%
  collect()

post_op_follow_up <- linked_data %>% 
  filter(t_op >=1) %>%
  group_by(census_id_final) %>% 
  mutate(follow_up = max(t_op) - min(t_op) + 1) %>%
  ungroup() %>%
  select(census_id_final, follow_up) %>%
  distinct() %>%
  summarise(average_follow_up = mean(follow_up),
            median_follow_up = percentile(follow_up, 0.5),
           max_follow_up = max(follow_up),
           min_follow_up = min(follow_up)) %>%
  mutate(group = "post-surgery") %>%
  collect()

pre_op_follow_up <- linked_data %>% 
  filter(t_op <1) %>%
  group_by(census_id_final) %>% 
  mutate(follow_up = max(t_op) - min(t_op) + 1) %>%
  ungroup() %>%
  select(census_id_final, follow_up) %>%
  distinct() %>%
  summarise(average_follow_up = mean(follow_up),
            median_follow_up = percentile(follow_up, 0.5),
           max_follow_up = max(follow_up),
           min_follow_up = min(follow_up)) %>%
  mutate(group = "pre-surgery") %>%
  collect()


all_follow_up <- bind_rows(follow_up, post_op_follow_up, pre_op_follow_up)

data.frame(all_follow_up)


#------------------------------------------------------------------------------------------
## Save all summary data
#------------------------------------------------------------------------------------------

if (dataset == "bariatric"){
  # Save all the above results
  dataset_names <- list('sample_flow' = sf,
                        'age_distribution' = age_distribution_all,
                     'age_stats' = age_stats_all,
                       'pay_and_employment_by_age' = pay_and_employment_by_age,
                        'pay_and_employment_summary' = pay_and_employment_summary,
                    'categorical_counts_all_result' = categorical_counts_all_result,
                        'op_count' = op_count,
                        'op_count_by_code' = op_count_by_code,
                   'op_count_month' = op_count_month,
                     'pay_by_time_to_op' = pay_by_time_to_op,
                        'pay_by_cal_month' = pay_by_cal_month,
                       'follow_up' = all_follow_up)
}else{
    # Save all the above results
  dataset_names <- list('sample_flow' = sf,
                        'age_distribution' = age_distribution_all,
                     'age_stats' = age_stats_all,
                       'pay_and_employment_by_age' = pay_and_employment_by_age,
                     'pay_and_employment_summary' = pay_and_employment_summary,
                    'categorical_counts_all_result' = categorical_counts_all_result,
                        'pay_by_time_to_op' = pay_by_time_to_op,
                        'pay_by_cal_month' = pay_by_cal_month,
                       'follow_up' = all_follow_up)
}

write.xlsx(dataset_names, paste0(save_folder,"person_level_descriptive_stats_all_", dataset_date, ".xlsx"))

  
}