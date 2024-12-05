# read in main HES file
# get distribution
# read in non exposed files
# assign opdate based on distribution

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
library(zoo)
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
# allow cartesian joins to enable join of distribution values to dataset
#config$spark.sql.crossJoin.enabled="true"

sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")

#------------------------------------------------------------------------------------------
### Load/set files 
#------------------------------------------------------------------------------------------

source("<filepath>/functions.R")
outputs_folder <- "<filepath>"

HES_data_date <- "20240911"


#------------------------------------------------------------------------------------------
# Load in bariatric HES data and get distribution
#------------------------------------------------------------------------------------------

bariatric_hes <- sdf_sql(sc, paste0("SELECT op_date FROM hmrc_outcome.bariatric_hes_",
                                    HES_data_date))

# assign month nmber (0 is April 2014)
bariatric_hes <- bariatric_hes %>%
  mutate(op_month = floor(months_between(op_date, "2014-04-01")))

# get number of ops in each month
n_op_in_month <- bariatric_hes %>%
  group_by(op_month) %>%
  summarise(op_count = n()) %>%
  collect() %>%
  data.frame() %>%
  arrange(op_month)

# smooth counts to take away some noise
n_op_in_month$op_count_smooth <- rollapplyr(n_op_in_month$op_count, 3, mean, na.rm = TRUE, 
                                            partial = TRUE, fill = NA, align='center')

# plot to check smoothing
n_op_in_month_long <- pivot_longer(n_op_in_month, cols=c('op_count', 'op_count_smooth'),
                               names_to = 'line', values_to = 'n_op') %>%
                      mutate(line_name = ifelse(line == "op_count", "Raw count of operations",
                                               "Smoothed count of operations"))

write.csv(n_op_in_month_long, paste0(outputs_folder, "smoothed_op_counts.csv"), row.names=FALSE)

ggplot(n_op_in_month_long, aes(x=op_month, y=n_op, colour=line_name)) +
  geom_line() + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme_bw() +
  labs(x ="Month", y = "Number of operations") +
  scale_x_continuous(breaks = seq(0, 84, by = 12), 
                         labels = c("April 2014","April 2015","April 2016","April 2017",
                                   "April 2018","April 2019","April 2020", "April 2021")) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))  +
  theme(legend.title=element_blank())

ggsave(paste0(outputs_folder,"operations_by_month_with_smoothing.png"), width = 14, height = 4, units = "in")

# get the cumulative proportion of ops in a month
# make dummy to join on
total_ops <- sum(n_op_in_month$op_count)
n_op_in_month <- n_op_in_month %>%
  arrange(op_month) %>%
  mutate(cum_sum = cumsum(op_count_smooth)) %>%
  mutate(max_cum_prop = cum_sum/total_ops) %>%
  mutate(min_cum_prop = lag(max_cum_prop, default=0))


# lookup for joining on
op_date_dist <- select(n_op_in_month, -cum_sum, -op_count, -op_count_smooth)


#------------------------------------------------------------------------------------------
# Load in census, non bariatric data and add an opdate based on distribution
#------------------------------------------------------------------------------------------

census_non_bariatric_hes <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.census_non_bariatric_hes_",
                                              HES_data_date))

# assign op_date dates according to distribution in bariatric dataset
census_non_bariatric_hes_opdate <- assign_date_by_distribution(census_non_bariatric_hes, op_date_dist)

# save out
table_name = paste0('hmrc_outcome.census_non_bariatric_hes_with_opdate_', format(Sys.Date(), '%Y%m%d'))
tbl_change_db(sc, "hmrc_outcome")

#delete table if already exists 
sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

sdf_register(census_non_bariatric_hes_opdate , 'census_non_bariatric_hes_opdate')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM census_non_bariatric_hes_opdate')
DBI::dbExecute(sc, sql)


#------------------------------------------------------------------------------------------
# Plot all opdate distributions as a check
#------------------------------------------------------------------------------------------

bariatric_n_op_per_month <- bariatric_hes %>% 
    mutate(op_month = floor(months_between(op_date, "2014-04-01"))) %>%
    group_by(op_month) %>%
    summarise(Count = n()) %>%
    collect() %>% 
    mutate(dataset = "Main dataset (had bariatric surgery)",
         prop_people = Count/sum(Count))

census_non_bariatric_n_op_per_month <- census_non_bariatric_hes_opdate %>% 
    mutate(op_month = floor(months_between(op_date, "2014-04-01"))) %>%
    group_by(op_month) %>%
    summarise(Count = n()) %>%
    collect() %>% 
    mutate(dataset = "Non-exposed dataset (census, no bariatric surgery)",
         prop_people = Count/sum(Count))

# combine for plotting
n_op_per_month_df <- bind_rows(bariatric_n_op_per_month, census_non_bariatric_n_op_per_month)

write.csv(n_op_per_month_df, paste0(outputs_folder, "op_proportions_comparison_of_datasets.csv"), row.names=FALSE)

ggplot(n_op_per_month_df, aes(x=op_month, y=prop_people, colour = dataset)) +
  geom_line() + 
  labs(title="Proportion of 'operations' by month of operation",
        x ="Date", y = "Proportion of 'operations'")  +
  theme_bw() +
  theme(legend.title=element_blank(),
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  scale_x_continuous(breaks = seq(0, 108, by = 12), 
                         labels = c("April 2014","April 2015","April 2016","April 2017",
                                   "April 2018","April 2019","April 2020", "April 2021",
                                   "April 2022", "April 2023"))

ggsave(paste0(outputs_folder,"proportion_of_operations_by_month.png"), width = 14, height = 4, units = "in")

