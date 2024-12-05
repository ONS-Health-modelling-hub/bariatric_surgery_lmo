################################################################################################
#                                   SAMPLE FLOW FUNCTIONS
################################################################################################


#-------------------------------------------------------------------------------------------------
# Create sample flow dataframe
#-------------------------------------------------------------------------------------------------

initialise_sample_flow <- function(){
  sample_flow <- data.frame(stage = character(0),
                          count = character(0))
  
  return(sample_flow)
}


#-------------------------------------------------------------------------------------------------
# Add to sample flow dataframe
#-------------------------------------------------------------------------------------------------

add_to_sample_flow <- function(df, sample_flow, rows = TRUE, row_label = NA, 
                        id_col = NA, id_label = NA){
  
  # add row to sample flow with number of rows in dataframe
  if (rows == TRUE){
    n_row <- as.character(sdf_nrow(df))
    if (is.na(row_label)){
        row_label <- paste0("Number of rows")
    }
    sample_flow <- sample_flow %>%
               dplyr::bind_rows(data.frame(stage = row_label,
                                          count = n_row))
  }
    
  # add row to sample flow with number of different ids (e.g. person ids)
  if (deparse(substitute(id_col)) %in% colnames(df)){
      n_id <- as.character(sdf_nrow(distinct(df, {{ id_col }})))
      if (is.na(id_label)){
          id_label <- paste0("Number of distinct ", deparse(substitute(id_col)))
      }
      sample_flow <- sample_flow %>%
               dplyr::bind_rows(data.frame(stage = id_label,
                                          count = n_id))
  }
  
  return(sample_flow)
}


add_custom_rows_to_sample_flow <- function(sample_flow, row_labels, counts){
  
    sample_flow <- sample_flow %>%
               dplyr::bind_rows(data.frame(stage = row_labels,
                                          count = counts))

  return(sample_flow)
}



#From IAPT project 
#Original authors: Klaudia Rzepnicka, Megan Lyons, Marta Rossa, Becca Smith

#-------------------------------------------------------------------------------------------------
# Add column for number of months passed (add_months_diff_col)
#-------------------------------------------------------------------------------------------------

#' Add column for number of months passed.
#' 
#' Adds column to data frame using a reference date column and column of interest. If the date
#' in the column of interest is before the reference date or is empty, it will be populated
#' with zero. 
#'
#' @param data Data frame.                                  
#' @param colname Unquoted name of column containing date of occurence of interest.
#' @param ref_col Unquoted name of column contraining the reference date for time passed.
#' Default is ref_date.
#' @param floor Boolean (TRUE or FALSE) of whether values should be floored. Default is TRUE
#' @param neg_to_zero Boolean (TRUE or FALSE) of whether negative values should be changed
#' to 0. Default is TRUE.
#' @return Data frame with added column `t_colname`
#' 
add_months_diff_col <- function(data,
                                colname,
                                ref_col = ref_date,
                                floor = TRUE,
                                neg_to_zero = FALSE) {
  
  data %>%
  dplyr::mutate(diff = months_between({{ ref_col }}, {{ colname }}) +1 ,
                diff = ifelse(floor, floor(diff), diff),
                "t_{{ colname }}" :=  ifelse(diff > 0, diff, ifelse(neg_to_zero, 0, diff))) %>%
  select(-diff)
}

#-------------------------------------------------------------------------------------------------
# Create vector of monthly dates (create_monthly_dates_vec)
#-------------------------------------------------------------------------------------------------
#' Create vector of monthly dates.
#' 
#' Create a vector containing the last day of every month within provided dates
#'
#' @param start_yr_month Character of date for start month in form "YYYY-MM"
#' @param end_yr_month Character of date for last month in form "YYYY-MM"
#' @return Vector of dates
#'
create_monthly_dates_vec <- function(start_yr_month, end_yr_month) {
  min_date <- as.Date(paste0(start_yr_month, "-01"))
  max_date <- as.Date(paste0(end_yr_month, "-01"))
  
  n_months <- lubridate::interval(min_date, max_date) %/% months(1)
  
  c(min_date + months(1:n_months)) - days(1)
}

#-------------------------------------------------------------------------------------------------
#Create long dataframe with monthly rows (create_long_monthly_df)
#-------------------------------------------------------------------------------------------------

#' Create long dataframe with monthly rows
#' 
#' @param data Data frame.
#' @param date_vec Vector of dates to be used when pivoting longer
#' @return Data frame with extra column 'ref_date', where the number of rows 
#' will be the length of date_vec times more than input data frame.
#'
create_long_monthly_df <- function(data, date_vec) {
  
  for (i in 1:length(date_vec)) {
    date <- as.character(date_vec[i])
    data <- mutate(data, "t_{i}" := date)
}
  data %>%
  pivot_longer(cols = starts_with("t_"), values_to ="ref_date") %>%
  select(-name)
}


#-------------------------------------------------------------------------------------------------
# Main monthly function
#-------------------------------------------------------------------------------------------------
#' @description This function makes monthly data out of the person level data for given year/data. 
#'
#' This function has other nested functions:
#'  - create_long_monthly_df
#'  - add_months_diff_col
#'
#' @param df = financial year person level data going in 
#' @return Spark DataFrame with monthly NHS TT data (with Census & deaths)

main_monthly_function <- function(df) {

#Date vector start and end dates for HES 
date_vec <- create_monthly_dates_vec(substr(follow_up_start_date, 1, 7), substr(follow_up_end_date, 1, 7))

monthly_person_level_df <- df %>%
                          # create monthly ref months
                           create_long_monthly_df(date_vec) %>%
                          # last day of month of operation
                          mutate(op_date_ = last_day(op_date)) %>% 
                          # n months between ref month and op month (ref_date - op_date so 
                          # positive for ref dates after op and negative for ref dates before op)
                          # remove _date from the and of the col name (it becomes t_op_
                           add_months_diff_col(op_date_) %>%
                           rename_with(~ gsub("_date", "", .x), starts_with("t_")) %>% 
                           arrange(census_final_id, ref_date) %>%
                          # create month year label for ref month
                           mutate(date_month = month(ref_date),
                                  date_year = year(ref_date)) %>%
                           mutate(month_label = paste0(date_year, "-", date_month)) %>% 
                           select(-c('date_month','date_year')) %>%
  
                            # if person died keep months before month of death
                           filter(is.na(dod) | dod > ref_date) %>%
 
                           mutate(ref_year = as.numeric(substring(ref_date,1,4)),
                                  dob_year = as.numeric(substring(prov_dob,1,4))) %>%
  
                          # create monthly age 
                           mutate(age_monthly = ifelse(substring(ref_date,6,10) >= substring(prov_dob,6,10), 
                                                       ref_year - dob_year, (ref_year - dob_year) -1)) %>%
  
                          filter(age_monthly >= 21 & age_monthly <= 69) %>%
                           select(-c('ref_year', 'dob_year', 'op_date_')) %>%

                          rename(t_op = t_op_) %>%
  
                          # limit the data to 5 years pre and post operation 
                           filter(t_op >= -59 & t_op <= 60)
                
  return(monthly_person_level_df)
}


#-------------------------------------------------------------------------------------------------
# Correct age band
#-------------------------------------------------------------------------------------------------
#' @description This function makes age bands from age_at_epistart
#' 
#'
#' @param df = person level data with age_at_epistart (linked_data_nhs_cen_nino)
#' @return Spark DataFrame with corrected age_bands column

correct_age_bands <- function(data) {
new_data <- data %>% 
      mutate(age_bands = case_when(age_at_operation >= 25 & age_at_operation < 35 ~ "25-34",
                            age_at_operation >= 35 & age_at_operation < 45 ~ "35-44",
                            age_at_operation >= 45 & age_at_operation < 55 ~ "45-54",
                            age_at_operation >= 55 & age_at_operation < 65 ~ "55-64",
                                  TRUE ~ "Out of age range"))


  return(new_data)
  
  
  
}



################################################################################################
#                                   CENSUS FUNCTIONS 
################################################################################################

#-------------------------------------------------------------------------------------------------
#Creates 'provisional' date of birth based on dob_quarter for census 2021 (create_dob_21)
#-------------------------------------------------------------------------------------------------

#' Create provisional date of birth based on dob_quarter
#' 
#' @param data Data frame.
#' @return Data frame with extra column 'prov_dob'
#'

create_dob_21 <- function(data) {
  
  data <- data %>%
        mutate(prov_dob = case_when(grepl('Q1', dob_quarter) ~ paste0(substring(dob_quarter,1,5), "02-15"),
                                      grepl('Q2', dob_quarter) ~ paste0(substring(dob_quarter,1,5), "05-15"),
                                      grepl('Q3', dob_quarter) ~ paste0(substring(dob_quarter,1,5), "08-15"),
                                      grepl('Q4', dob_quarter) ~ paste0(substring(dob_quarter,1,5), "11-15"),
                                      TRUE ~ 'error')) %>%
        mutate(prov_dob = to_date(prov_dob, "yyyy-MM-dd"))
               

  return(data)
  
  
  
}

#-------------------------------------------------------------------------------------------------
#Creates 'provisional' date of birth based on dob_quarter for census 2011 (create_dob_11)
#-------------------------------------------------------------------------------------------------

#' Create provisional date of birth based on dob_quarter
#' 
#' @param data Data frame.
#' @return Data frame with extra column 'prov_dob'
#'

create_dob_11 <- function(data) {
  
  data <- data %>%
        mutate(prov_dob = case_when(substring(dob_quarter,6,7)== "01" ~ paste0(substring(dob_quarter,1,5),"02-15"),
                                      substring(dob_quarter,6,7)==  "02" ~ paste0(substring(dob_quarter,1,5),"05-15"),
                                      substring(dob_quarter,6,7)== "03" ~ paste0(substring(dob_quarter,1,5),"08-15"),
                                      substring(dob_quarter,6,7)== "04" ~ paste0(substring(dob_quarter,1,5),"11-15"),
                                      TRUE ~ 'error')) %>%
        mutate(prov_dob = to_date(prov_dob, "yyyy-MM-dd"))
               

  return(data)
  
  
  
}


#-------------------------------------------------------------------------------------------------
#Renames and aggregates census21 variables (rename_census21_variables)
#-------------------------------------------------------------------------------------------------

rename_census21_variables <- function(data){
  
new_data <- data %>%
mutate(census_sex = case_when(sex == 1 ~ 'Females',
                       sex == 2 ~ 'Males'),
       
       
  
      ethnic_group_tb = case_when(ethnic_group_tb >= 01 & ethnic_group_tb <= 05 ~ "White",
                                   ethnic_group_tb >= 06 & ethnic_group_tb <= 09 ~ "Mixed",
                                   ethnic_group_tb >= 10 & ethnic_group_tb <= 14 ~ "Asian",
                                   ethnic_group_tb >= 15 & ethnic_group_tb <= 17 ~ "Black",
                                   ethnic_group_tb >= 18 & ethnic_group_tb <= 19 ~ "Other",
                                   ethnic_group_tb == -8 ~ "Missing or not stated",
                                 TRUE ~ "Missing or not stated"),
       
        disability_category = case_when(disability == 1 ~ "Yes - reduced a lot",
                                disability == 2 ~ "Yes - reduced a little",
                                disability == 3 | disability == 4 ~ "No",
                                TRUE ~ "Missing or not stated"),
       
       disability_binary = case_when(disability >= 1 & disability <= 2 ~ as.integer(1),
                                     disability == 3 | disability == 4 ~ as.integer(0),
                                    TRUE ~ as.integer(0)),

       
        ns_sec = case_when(ns_sec >= 1 & ns_sec <= 3 ~ "Higher managerial, administrative and professional occupations",
                           ns_sec >= 4 & ns_sec <= 6 ~ "Lower managerial, administrative and professional occupations",
                           ns_sec == 7 ~ "Intermediate occupations",
                           ns_sec >= 8 & ns_sec <= 9 ~ "Small employers and own account workers",
                           ns_sec >= 10 & ns_sec <= 11 ~ "Lower supervisory and technical occupations",
                           ns_sec == 12 ~ "Semi-routine occupations",
                           ns_sec == 13 ~ "Routine occupations",
                           ns_sec == 14 ~ "Never worked",
                           ns_sec == 15 ~ "Long-term unemployed",
                           ns_sec == 16 ~ "Full-Time students",
                           ns_sec == -8 ~ "Missing or not stated",
                          TRUE ~ "Missing or not stated"),
       
       


        religion_tb = case_when(religion_tb == 1 ~ "No religion",
                                 religion_tb == 2 ~ "Christian",
                                 religion_tb == 3 ~ "Buddhist",
                                 religion_tb == 4 ~ "Hindu",
                                 religion_tb == 5 ~ "Jewish",
                                 religion_tb == 6 ~ "Muslim",
                                 religion_tb == 7 ~ "Sikh",
                                 religion_tb == 8 ~ "Other religion",
                                 religion_tb == 9 ~ "Missing or not stated",
                               TRUE ~ "Missing or not stated"),

    
       highest_qualification = case_when(highest_qualification == 0 ~   "No qualifications",
                                  highest_qualification == 1 ~  "Level 1 qualifications",
                                  highest_qualification == 2 ~  "Level 2 qualifications",
                                  highest_qualification == 3 ~  "Apprenticeship",
                                  highest_qualification == 4 ~  "Level 3 qualifications",
                                  highest_qualification == 5 ~  "Level 4 qualifications and above",
                                  highest_qualification == 6 ~  "Other: Vocational/Work-related qualifications/Foreign qualifications)",
                                  highest_qualification == -8 ~ "Missing or not stated",
                                        TRUE ~ "Missing or not stated"),

       
      legal_partnership_status = case_when(legal_partnership_status == 1 ~ "Never married and never registered a civil partnership",
                                          legal_partnership_status >= 2 & legal_partnership_status <= 5 ~ "Married or in a registered civil partnership",
                                           legal_partnership_status >= 6 & legal_partnership_status <= 7 ~ "Separated, but still legally married or still legally in a civil partnership",
                                           legal_partnership_status >= 8 & legal_partnership_status <= 9 ~ "Divorced or civil partnership dissolved",
                                           legal_partnership_status >= 10 & legal_partnership_status <= 11 ~ "Widowed or surviving civil partnership partner",
                                           TRUE ~ "Missing or not stated"),

  
       
       return(new_data)
       }


#-------------------------------------------------------------------------------------------------
#Renames and aggregates census11 variables (rename_census11_variables)
#-------------------------------------------------------------------------------------------------


rename_census11_variables <- function(data){
  
new_data <- data %>%
  mutate(nssec = as.numeric(nssec)) %>%
  mutate(census_sex = case_when(sex == 1 ~ 'Males',
                       sex == 2 ~ 'Females'),
       

  
      ethnic_group_tb = case_when(ethpuk11 >= 01 & ethpuk11 <= 04 ~ "White",
                                   ethpuk11 >= 05 & ethpuk11 <= 08 ~ "Mixed",
                                   ethpuk11 >= 09 & ethpuk11 <= 13 ~ "Asian",
                                   ethpuk11 >= 14 & ethpuk11 <= 16 ~ "Black",
                                   ethpuk11 >= 17 & ethpuk11 <= 18 ~ "Other",
                                   ethpuk11 == 'XX' ~ "Missing or not stated",
                                 TRUE ~ "Missing or not stated"),
       
        disability_category = case_when(disability == 1 ~ "Yes - reduced a lot",
                                disability == 2 ~ "Yes - reduced a little",
                                disability == 3  ~ "No",
                                disability == 'X' ~ "Missing or not stated",
                                TRUE ~ "Missing or not stated"),
       
       disability_binary = case_when(disability >= 1 & disability <= 2 ~ as.integer(1),
                                     disability == 3 ~ as.integer(0),
                                     disability == 'X' ~ as.integer(0),
                                    TRUE ~ as.integer(0)),


        ns_sec = case_when(nssec >= 1 & nssec < 4 ~ "Higher managerial, administrative and professional occupations",
                           nssec >= 4 & nssec < 7 ~ "Lower managerial, administrative and professional occupations",
                           nssec >= 7 & nssec <8 ~ "Intermediate occupations",
                           nssec >= 8 & nssec < 10 ~ "Small employers and own account workers",
                           nssec >= 10 & nssec < 12 ~ "Lower supervisory and technical occupations",
                           nssec >= 12 & nssec <13 ~ "Semi-routine occupations",
                           nssec >= 13 & nssec <14 ~ "Routine occupations",
                           nssec == 14.1 ~ "Never worked",
                           nssec == 14.2 ~ "Long-term unemployed",
                           nssec >= 15 & nssec <16 ~ "Full-Time students",
                           nssec >= 16 & nssec <17 ~ "Missing or not stated",
                           nssec == 17 ~ "Missing or not stated",
                           nssec == 'XXXX' ~ "Missing or not stated",
                          TRUE ~ "Missing or not stated"),


        religion_tb = case_when(relpuk11 == 1 ~ "No religion",
                                 relpuk11 == 2 ~ "Christian",
                                 relpuk11 == 3 ~ "Buddhist",
                                 relpuk11 == 4 ~ "Hindu",
                                 relpuk11 == 5 ~ "Jewish",
                                 relpuk11 == 6 ~ "Muslim",
                                 relpuk11 == 7 ~ "Sikh",
                                 relpuk11 == 8 ~ "Other religion",
                                 relpuk11 == 9 ~ "Missing or not stated",
                                 relpuk11 == 'X' ~ "Missing or not stated",
                               TRUE ~ "Missing or not stated"),

    
       highest_qualification = case_when(hlqpuk11 == 10 ~   "No qualifications",
                                  hlqpuk11 == 11 ~  "Level 1 qualifications",
                                  hlqpuk11 == 12 ~  "Level 2 qualifications",
                                  hlqpuk11 == 13 ~  "Apprenticeship",
                                  hlqpuk11 == 14 ~  "Level 3 qualifications",
                                  hlqpuk11 == 15 ~  "Level 4 qualifications and above",
                                  hlqpuk11 == 16 ~  "Other: Vocational/Work-related qualifications/Foreign qualifications)",
                                  hlqpuk11 == 'XX' ~ "Missing or not stated",
                                        TRUE ~ "Missing or not stated"),

       legal_partnership_status = case_when(marstat == 1 ~ "Never married and never registered a civil partnership",
                                            marstat == 2 | marstat == 6 ~ "Married or in a registered civil partnership",
                                            marstat == 3 | marstat == 7 ~ "Separated, but still legally married or still legally in a civil partnership",
                                            marstat == 4 | marstat == 8 ~ "Divorced or civil partnership dissolved",
                                            marstat == 5 | marstat == 9 ~ "Widowed or surviving civil partnership partner",
                                            TRUE ~ "Missing or not stated"),

       return(new_data)
       }
    


################################################################################################
#                                   DESCRIPTIVE FUNCTIONS 
################################################################################################


# Calculating counts for categorical variables for all people in df 

calculate_var_count <- function(category_column) {
    df <- linked_data_person_level %>% select(census_id_final, all_of(category_column))
 
  total = sdf_nrow(df)
  
  result <- df %>%
    group_by(Category_name = !!sym(category_column)) %>%
    summarise(Count = n(), Percentage = round(n()/total*100,1)) %>%
    mutate(Variable_names = category_column)  %>%
  select(Variable_names, everything())
  return(result)
}


# Calculating counts for categorical variables for op type

  calculate_2vars_counts <- function(vars) {
    df <- linked_data_person_level %>% select(census_id_final, op_name, vars) %>%
    sdf_drop_duplicates(cols = "census_id_final") 
    
  df2 <- df %>% group_by(across(c(2,3))) %>%
    summarise(Count = n()) %>%
    ungroup() %>%
    rename(grouping = 2) %>%
    mutate(characteristic = vars) %>%
    as.data.frame()
    
  return(df2)
}


# standardised differences
std_diffs <- function(df, cols_category, cols_numeric, cols_binary){
    
    std_diff_df <- data.frame(variable = character(), 
                          std_diff=numeric(),
                          std_diff_ucl=numeric(), 
                          std_diff_lcl=numeric())

    for (col in cols_category){
  
      col_number <- which(colnames(df)==col)
  
      std_diff <- stddiff.category(df, 1, c(col_number,col_number))

      std_diff_row <- data.frame(variable = col, 
                             std_diff=std_diff[1,5],
                            std_diff_lcl=std_diff[1,6],
                            std_diff_ucl=std_diff[1,7])
  
      std_diff_df <- bind_rows(std_diff_df, std_diff_row)
    }
  
  
      for (col in cols_numeric){
  
      col_number <- which(colnames(df)==col)
  
      std_diff <- stddiff.numeric(df, 1, c(col_number,col_number))

      std_diff_row <- data.frame(variable = col, 
                             std_diff=std_diff[1,7],
                            std_diff_lcl=std_diff[1,8],
                            std_diff_ucl=std_diff[1,9])
  
      std_diff_df <- bind_rows(std_diff_df, std_diff_row)

    }
    
    return(std_diff_df)
}




################################################################################################
#                                       STRATA AND DATE ASSIGNMENT FUNCTIONS                                       # 
################################################################################################

#-------------------------------------------------------------------------------------------------
# Add operation date to non exposed samples according to a monthly distribution of dates
#-------------------------------------------------------------------------------------------------

#' Add column for operation date.
#' 
#' Adds column of dates to dataframe where the date is randomly assigned according to
#' a distribution of cumulative proportion of dates in each month.
#'
#' @param df Spark dataframe to add operation dates to.                                  
#' @param distribution Dataframe containing column op_month for the month number,
#' min_cum_prop and max_cum_prop for the minimum and maximum values of the cumulative 
#' probability distribution in that month
#' @return Dataframe with added column `op_date`
#' 
assign_date_by_distribution <- function(df, distribution){
  # assign a column with a random number between 0 and 1
  num_rows <- sdf_nrow(df)
  rand_df <- sdf_runif(sc, num_rows, min=0, max=1, output_col="rand_num")
  df_new <- sdf_bind_cols(df, rand_df)

  # make dummy to join on
  df_new <- mutate(df_new, dummy=TRUE)
  
  # max up to 1 for the distribution so all random numbers have a match
  distribution <- distribution %>%
    mutate(max_cum_prop = ifelse(op_month==max(op_month), 1, max_cum_prop))
  
  # make spark dataframe for joining  
  distribution_spk <- copy_to(sc, distribution, overwrite = TRUE)
  distribution_spk <- mutate(distribution_spk, dummy=TRUE)

  # join on and filter to get matching month number
  df_joined <- left_join(df_new, distribution_spk, by='dummy')

  df_joined <- df_joined %>% 
    filter(rand_num > min_cum_prop & rand_num <= max_cum_prop) %>%
    select(-dummy, -rand_num, -max_cum_prop, -min_cum_prop)

  # assign mid date of month as op date
  df_joined <- df_joined %>%
    mutate(op_date = add_months("2014-04-15", op_month))
  
  return(df_joined)
}

#-------------------------------------------------------------------------------------------------
# Create strata samples SPARK (create_strata_sample)
#-------------------------------------------------------------------------------------------------
#' Creates dataframe with random sample of strata population to fit required proportions
#' 
#' @param df dataframe containing population data
#' @param sex Sex group of interest e.g. Males/Females
#' @param age age_bands group of interest
#' @param strata_number 1:18; follows order grouping by census_sex and then age_bands 
#'                            e.g 1 = Females 21-24, 2 = Females 25-29 etc
#' @return Dataframe
#'

create_strata_sample_spark <- function(df, sex, age, strata_number, total_sample_size) {

  strata <- df %>% filter(census_sex == {{sex}} & five_year_bands == {{age}})
  
  sample_size <- round(total_sample_size * strata_proportions[{{strata_number}}],0)
  
  strata_sample_fraction <- (sample_size/sdf_nrow(strata)) + 0.1
  
  if (strata_sample_fraction > 1) {
  
    print("Error: required strata sample size is larger than strata population size")
  }

  else {
    strata_sample <- strata %>%
                sdf_sample(fraction = strata_sample_fraction, replacement = FALSE) %>%
                 head(n = sample_size)

    return(strata_sample) }
}





#-------------------------------------------------------------------------------------------------
# Create strata samples  (create_strata_sample)
#-------------------------------------------------------------------------------------------------
#' Creates dataframe with random sample of strata population to fit required proportions
#' Dataframe must be saved in memory for this to work
#' 
#' @param df dataframe containing population data
#'
#' @return Dataframe
#'

create_strata_sample <- function(df, total_sample_size){
  
  #Create samples for each strata
  strata1 <- create_strata_sample_spark(df, 'Females', "25-29", 1, total_sample_size)
  strata2 <- create_strata_sample_spark(df, 'Females', "30-34", 2, total_sample_size)
  strata3 <- create_strata_sample_spark(df, 'Females', "35-39", 3, total_sample_size)
  strata4 <- create_strata_sample_spark(df, 'Females', "40-44", 4, total_sample_size)
  strata5 <- create_strata_sample_spark(df, 'Females', "45-49", 5, total_sample_size)
  strata6 <- create_strata_sample_spark(df, 'Females', "50-54", 6, total_sample_size)
  strata7 <- create_strata_sample_spark(df, 'Females', "55-59", 7, total_sample_size)
  strata8 <- create_strata_sample_spark(df, 'Females', "60-64", 8, total_sample_size)

  strata9 <- create_strata_sample_spark(df, 'Males', "25-29", 9, total_sample_size)
  strata10 <- create_strata_sample_spark(df, 'Males', "30-34", 10, total_sample_size)
  strata11 <- create_strata_sample_spark(df, 'Males', "35-39", 11, total_sample_size)
  strata12 <- create_strata_sample_spark(df, 'Males', "40-44", 12, total_sample_size)
  strata13 <- create_strata_sample_spark(df, 'Males', "45-49", 13, total_sample_size)
  strata14 <- create_strata_sample_spark(df, 'Males', "50-54", 14, total_sample_size)
  strata15 <- create_strata_sample_spark(df, 'Males', "55-59", 15, total_sample_size)
  strata16 <- create_strata_sample_spark(df, 'Males', "60-64", 16, total_sample_size)


  #Bind to create full stratified sample
  
  full_sample <- strata1
  full_sample <- rbind(full_sample, strata2)
  full_sample <- rbind(full_sample, strata3)
  full_sample <- rbind(full_sample, strata4)
  full_sample <- rbind(full_sample, strata5)
  full_sample <- rbind(full_sample, strata6)
  full_sample <- rbind(full_sample, strata7)
  full_sample <- rbind(full_sample, strata8)
  full_sample <- rbind(full_sample, strata9)
  full_sample <- rbind(full_sample, strata10)
  full_sample <- rbind(full_sample, strata11)
  full_sample <- rbind(full_sample, strata12)
  full_sample <- rbind(full_sample, strata13)
  full_sample <- rbind(full_sample, strata14)
  full_sample <- rbind(full_sample, strata15)
  full_sample <- rbind(full_sample, strata16)

  print(sdf_nrow(full_sample))
  
  rm(strata1, strata2, strata3,strata4, strata5,strata6, strata7, strata8, strata9,
    strata10,strata11,strata12,strata13,strata14,strata15,strata16)

  return(full_sample)
}


