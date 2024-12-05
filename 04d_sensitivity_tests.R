### SENSITIVITY TESTS ###

Change baseline period length
Change baseline period timing
Omit COVID-19 dates


### PLACEBO TESTS ###

Set 'surgery' to before surgery and censory at actual surgery DONE NEED EXPORT PLOTS
Split non exposed data in 2, 'treat' one to look at trends



source("<filepath>/04a_make_analysis_dataset.R")

# create results folders
for (i in c("baseline_length", "baseline_timing",
           "no_covid")){
  dir.create(file.path(paste0(outputs_folder, "/", "sensitivity_tests"), i))
}

#------------------------------------------------------------------------------------------
### MAKE IN WORK DATASET AND SET MAIN MODEL FORMULAE
#------------------------------------------------------------------------------------------

df_combined_non_bariatric_in_work <- filter(df_combined_non_bariatric, in_work==1)

main_model_pay <- "pay_deflated ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"

main_model_employment <- "in_work ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"


##main model results for comoparison
main_model_pay_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_overall_exposed_and_non_bariatric.csv"))
main_model_pay_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_overall_exposed_and_non_bariatric_model_params.csv"))

main_model_pay_in_work_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_in_work_exposed_and_non_bariatric.csv"))
main_model_pay_in_work_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_in_work_exposed_and_non_bariatric_model_params.csv"))

main_model_employment_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_employment_exposed_and_non_bariatric.csv"))
main_model_employment_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_employment_exposed_and_non_bariatric_model_params.csv"))



#------------------------------------------------------------------------------------------
### CHANGE BASELINE LENGTH TO 12 OR 24 MONTHS BEFORE SURGERY
#------------------------------------------------------------------------------------------

# extend baseline period to include 12 months pre-surgery
df_combined_non_bariatric_extended_baseline_12 <- df_combined_non_bariatric %>%
  mutate(t_treatment_6_month = as.factor(ifelse(as.character(t_treatment_6_month) =="-1", "0", 
                                      as.character(t_treatment_6_month))))

df_combined_non_bariatric_extended_baseline_12$t_treatment_6_month <- relevel(df_combined_non_bariatric_extended_baseline_12$t_treatment_6_month, "0")

# extend baseline period to include 24 months pre-surgery
df_combined_non_bariatric_extended_baseline_24 <- df_combined_non_bariatric %>%
  mutate(t_treatment_6_month = as.factor(ifelse(as.character(t_treatment_6_month) =="-1" |
                                                as.character(t_treatment_6_month) =="-2" |
                                                as.character(t_treatment_6_month) =="-3", "0", 
                                      as.character(t_treatment_6_month))))

df_combined_non_bariatric_extended_baseline_24$t_treatment_6_month <- relevel(df_combined_non_bariatric_extended_baseline_24$t_treatment_6_month, "0")


########### PAY

pay_12_month_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_extended_baseline_12, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a12_month_baseline_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay (overall)")

pay_24_month_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_extended_baseline_24, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a24_month_baseline_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

# compare main model to both sensitivity test models



pay_comparison_12 <- compare_model_sig_tables(model_2_results = pay_12_month_baseline$results,
                        model_2_params = pay_12_month_baseline$params,
                        model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_name = "Baseline_period_12_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a12_month_baseline_effect_on_pay_overall_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length")

pay_comparison_24 <- compare_model_sig_tables(model_2_results = pay_24_month_baseline$results,
                        model_2_params = pay_24_month_baseline$params,
                        model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_name = "Baseline_period_24_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a24_month_baseline_effect_on_pay_overall_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length")

# combine
pay_comparison <- left_join(pay_comparison_12, pay_comparison_24, by=c("output", "Main_model_pay"))


############ PAY IN WORK

df_combined_non_bariatric_extended_baseline_in_work_12 <- filter(df_combined_non_bariatric_extended_baseline_12, in_work==1)
pay_in_work_12_month_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_extended_baseline_in_work_12, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a12_month_baseline_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay among those in work")
  

pay_in_work_comparison_12 <- compare_model_sig_tables(model_2_results = pay_in_work_12_month_baseline$results,
                        model_2_params = pay_in_work_12_month_baseline$params,
                        model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_name = "Baseline_period_12_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a12_month_baseline_effect_on_pay_in_work_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length")

df_combined_non_bariatric_extended_baseline_in_work_24 <- filter(df_combined_non_bariatric_extended_baseline_24, in_work==1)
pay_in_work_24_month_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_extended_baseline_in_work_24, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a24_month_baseline_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

pay_in_work_comparison_24 <- compare_model_sig_tables(model_2_results = pay_in_work_24_month_baseline$results,
                        model_2_params = pay_in_work_24_month_baseline$params,
                        model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_name = "Baseline_period_24_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a24_month_baseline_effect_on_pay_in_work_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length")

# combine
pay_in_work_comparison <- left_join(pay_in_work_comparison_12, pay_in_work_comparison_24, by=c("output", "Main_model_pay"))
  

############# EMPLOYMENT

employment_12_month_baseline <- run_model_employment_pretreatment(df_combined_non_bariatric_extended_baseline_12, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a12_month_baseline_effect_on_employment_exposed_and_non_bariatric"),
                           y_lims=c(-2,6),
                            plt_title = "Probability of employment")

employment_comparison_12 <- compare_model_sig_tables(model_2_results = employment_12_month_baseline$results,
                        model_2_params = employment_12_month_baseline$params,
                        model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_name = "Baseline_period_12_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a12_month_baseline_effect_on_employment_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length",
                        employment = T)

employment_24_month_baseline <- run_model_employment_pretreatment(df_combined_non_bariatric_extended_baseline_24, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/baseline_length",
                           save_name = paste0("a24_month_baseline_effect_on_employment_exposed_and_non_bariatric"),
                           y_lims=c(-2,6))


employment_comparison_24 <- compare_model_sig_tables(model_2_results = employment_24_month_baseline$results,
                        model_2_params = employment_24_month_baseline$params,
                        model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_name = "Baseline_period_24_months",
                        model_1_name = "Main_model_pay",
                        save_name = "a24_month_baseline_effect_on_employment_exposed_and_non_bariatric_comparison_table",
                        save_folder = "sensitivity_tests/baseline_length",
                        employment = T)

# combine
employment_comparison <- left_join(employment_comparison_12, employment_comparison_24, by=c("output", "Main_model_pay"))
  


# combined plots
combine_plots(pay_12_month_baseline$plot, 
              pay_in_work_12_month_baseline$plot,                
              employment_12_month_baseline$plot,
              save_folder = "sensitivity_tests/baseline_length",
              save_name = "baseline_period_12_months")

combine_plots(pay_24_month_baseline$plot, 
              pay_in_work_24_month_baseline$plot,                
              employment_24_month_baseline$plot,
              save_folder = "sensitivity_tests/baseline_length",
              save_name = "baseline_period_24_months")


# combine params
all_params <- bind_rows(pay_12_month_baseline$params, pay_in_work_12_month_baseline$params, 
                        employment_12_month_baseline$params, pay_24_month_baseline$params, pay_in_work_24_month_baseline$params, 
                        employment_24_month_baseline$params)


# save out all results
data_to_save <- list('pay_overall_12_month' = pay_12_month_baseline$results,
                      'pay_in_work_12_month' = pay_in_work_12_month_baseline$results,
                      'employment_12_month' = employment_12_month_baseline$results,
                     'pay_overall_24_month' = pay_24_month_baseline$results,
                      'pay_in_work_24_month' = pay_in_work_24_month_baseline$results,
                      'employment_24_month' = employment_24_month_baseline$results,
                      'params' = all_params,
                    'pay_comparison' = pay_comparison,
                    'pay_in_work_comparison' = pay_in_work_comparison,
                    'employment_comparison' = employment_comparison)

write.xlsx(data_to_save, 
             paste0(outputs_folder,"sensitivity_tests/baseline_length/extended_baseline_all_results_exposed_and_non_bariatric.xlsx"))



#------------------------------------------------------------------------------------------
### CHANGE BASELINE TIMING
#------------------------------------------------------------------------------------------

######## make datasets with different baselines

# set value for non exposed to be -1 (instead of 0), so 7-12 months before month of operation
df_combined_non_bariatric_change_baseline_minus_1 <- df_combined_non_bariatric %>%
  mutate(t_treatment_6_month = as.factor(ifelse(treat == 0, "-1",as.character(t_treatment_6_month))))

# change baseline period to months 6-11 before surgery (6 months earlier than current)
df_combined_non_bariatric_change_baseline_minus_1$t_treatment_6_month <- relevel(df_combined_non_bariatric_change_baseline_minus_1$t_treatment_6_month, "-1")


# set value for non exposed to be -2 (instead of 0), so 13-18 months before month of operation
df_combined_non_bariatric_change_baseline_minus_2 <- df_combined_non_bariatric %>%
  mutate(t_treatment_12_month = as.factor(ifelse(treat == 0, "-2",as.character(t_treatment_6_month))))

# change baseline period to months 6-11 before surgery (6 months earlier than current)
df_combined_non_bariatric_change_baseline_minus_2$t_treatment_6_month <- relevel(df_combined_non_bariatric_change_baseline_minus_2$t_treatment_6_month, "-2")

# set value for non exposed to be -4 (instead of 0), so 2 years earlier
df_combined_non_bariatric_change_baseline_minus_4 <- df_combined_non_bariatric %>%
  mutate(t_treatment_6_month = as.factor(ifelse(treat == 0, "-4",as.character(t_treatment_6_month))))

# change baseline period to 2 years earlier
df_combined_non_bariatric_change_baseline_minus_4$t_treatment_6_month <- relevel(df_combined_non_bariatric_change_baseline_minus_4$t_treatment_6_month, "-4")


#### PAY 

pay_6_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_minus_1, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_6m_earlier_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

pay_12_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_minus_2, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_12m_earlier_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

pay_24_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_minus_4, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_24m_earlier_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

# compare main model to both sensitivity test models

pay_comparison_6 <- compare_model_sig_tables(model_2_results = pay_6_month_earlier_baseline$results,
                        model_2_params = pay_6_month_earlier_baseline$params,
                        model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_name = "Baseline_period_6_months_earlier",
                        model_1_name = "Main_model_pay",
                        save_name = "baseline_6m_earlier_effect_on_pay_overall_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

pay_comparison_12 <- compare_model_sig_tables(model_2_results = pay_12_month_earlier_baseline$results,
                        model_2_params = pay_12_month_earlier_baseline$params,
                        model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_name = "Baseline_period_12_months_earlier",
                        model_1_name = "Main_model_pay",
                        save_name = "baseline_12m_earlier_effect_on_pay_overall_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

pay_comparison_24 <- compare_model_sig_tables(model_2_results = pay_24_month_earlier_baseline$results,
                        model_2_params = pay_24_month_earlier_baseline$params,
                        model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_name = "Baseline_period_24_months_earlier",
                        model_1_name = "Main_model_pay",
                        save_name = "baseline_24m_earlier_effect_on_pay_overall_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

# combine
pay_comparison <- left_join(pay_comparison_6, pay_comparison_12, by=c("output", "Main_model_pay"))
pay_comparison <- left_join(pay_comparison, pay_comparison_24, by=c("output", "Main_model_pay"))





#### PAY IN WORK

df_combined_non_bariatric_change_baseline_in_work_minus_1 <- filter(df_combined_non_bariatric_change_baseline_minus_1, in_work==1)
pay_in_work_6_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_in_work_minus_1, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_6m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))
  
df_combined_non_bariatric_change_baseline_in_work_minus_2 <- filter(df_combined_non_bariatric_change_baseline_minus_2, in_work==1)
pay_in_work_12_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_in_work_minus_2, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_12m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))

df_combined_non_bariatric_change_baseline_in_work_minus_4 <- filter(df_combined_non_bariatric_change_baseline_minus_4, in_work==1)
pay_in_work_24_month_earlier_baseline <- run_model_pay_pretreatment(df_combined_non_bariatric_change_baseline_in_work_minus_4, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_24m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150))
  

pay_in_work_comparison_6 <- compare_model_sig_tables(model_2_results = pay_in_work_6_month_earlier_baseline$results,
                        model_2_params = pay_in_work_6_month_earlier_baseline$params,
                        model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_name = "Baseline_period_6_months_earlier",
                        model_1_name = "Main_model_pay_in_work",
                        save_name = "baseline_6m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

pay_in_work_comparison_12 <- compare_model_sig_tables(model_2_results = pay_in_work_12_month_earlier_baseline$results,
                        model_2_params = pay_in_work_12_month_earlier_baseline$params,
                        model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_name = "Baseline_period_12_months_earlier",
                        model_1_name = "Main_model_pay_in_work",
                        save_name = "baseline_12m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

pay_in_work_comparison_24 <- compare_model_sig_tables(model_2_results = pay_in_work_24_month_earlier_baseline$results,
                        model_2_params = pay_in_work_24_month_earlier_baseline$params,
                        model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_name = "Baseline_period_24_months_earlier",
                        model_1_name = "Main_model_pay_in_work",
                        save_name = "baseline_24m_earlier_effect_on_pay_in_work_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing")

# combine
pay_in_work_comparison <- left_join(pay_in_work_comparison_6, pay_in_work_comparison_12, by=c("output", "Main_model_pay_in_work"))
pay_in_work_comparison <- left_join(pay_in_work_comparison, pay_in_work_comparison_24, by=c("output", "Main_model_pay_in_work"))



#### EMPLOYMENT

employment_6_month_earlier_baseline <- run_model_employment_pretreatment(df_combined_non_bariatric_change_baseline_minus_1, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_6m_earlier_effect_on_employment_exposed_and_non_bariatric"),
                            y_lims=c(-2,6))


employment_12_month_earlier_baseline <- run_model_employment_pretreatment(df_combined_non_bariatric_change_baseline_minus_2, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_12m_earlier_effect_on_employment_exposed_and_non_bariatric"),
                            y_lims=c(-2,6))

employment_24_month_earlier_baseline <- run_model_employment_pretreatment(df_combined_non_bariatric_change_baseline_minus_4, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/baseline_timing",
                           save_name = paste0("baseline_24m_earlier_effect_on_employment_exposed_and_non_bariatric"),
                            y_lims=c(-2,6))


employment_comparison_6 <- compare_model_sig_tables(model_2_results = employment_6_month_earlier_baseline$results,
                        model_2_params = employment_6_month_earlier_baseline$params,
                        model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_name = "Baseline_period_6_months_earlier",
                        model_1_name = "Main_model_employment",
                        save_name = "baseline_6m_earlier_effect_on_employment_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing",
                        employment = T)

employment_comparison_12 <- compare_model_sig_tables(model_2_results = employment_12_month_earlier_baseline$results,
                        model_2_params = employment_12_month_earlier_baseline$params,
                        model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_name = "Baseline_period_12_months_earlier",
                        model_1_name = "Main_model_employment",
                        save_name = "baseline_12m_earlier_effect_on_employment_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing",
                        employment = T)

employment_comparison_24 <- compare_model_sig_tables(model_2_results = employment_24_month_earlier_baseline$results,
                        model_2_params = employment_24_month_earlier_baseline$params,
                        model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_name = "Baseline_period_24_months_earlier",
                        model_1_name = "Main_model_employment",
                        save_name = "baseline_24m_earlier_effect_on_employment_exposed_and_non_bariatric",
                        save_folder = "sensitivity_tests/baseline_timing",
                        employment = T)

# combine
employment_comparison <- left_join(employment_comparison_6, employment_comparison_12, by=c("output", "Main_model_employment"))
employment_comparison <- left_join(employment_comparison, employment_comparison_24, by=c("output", "Main_model_employment"))



# combine params
all_params <- bind_rows(pay_6_month_earlier_baseline$params, 
                        pay_in_work_6_month_earlier_baseline$params, 
                        employment_6_month_earlier_baseline$params, 
                        pay_12_month_earlier_baseline$params, 
                        pay_in_work_12_month_earlier_baseline$params, 
                        employment_12_month_earlier_baseline$params, 
                        pay_24_month_earlier_baseline$params, 
                        pay_in_work_24_month_earlier_baseline$params, 
                        employment_24_month_earlier_baseline$params)


# save out all results
data_to_save <- list('pay_6_month_earlier_baseline' = pay_6_month_earlier_baseline$results,
                      'pay_in_work_6_month_earlier_baseline' = pay_in_work_6_month_earlier_baseline$results,
                      'employment_6_month_earlier_baseline' = employment_6_month_earlier_baseline$results,
                      'pay_12_month_earlier_baseline' = pay_12_month_earlier_baseline$results,
                      'pay_in_work_12_month_earlier_baseline' = pay_in_work_12_month_earlier_baseline$results,
                      'employment_12_month_earlier_baseline' = employment_12_month_earlier_baseline$results,
                     'pay_24_month_earlier_baseline' = pay_24_month_earlier_baseline$results,
                      'pay_in_work_24_month_earlier_baseline' = pay_in_work_24_month_earlier_baseline$results,
                      'employment_24_month_earlier_baseline' = employment_24_month_earlier_baseline$results,
                      'params' = all_params,
                    'pay_comparison' = pay_comparison,
                    'pay_in_work_comparison' = pay_in_work_comparison,
                    'pay_employment_comparison' = employment_comparison)

write.xlsx(data_to_save, 
             paste0(outputs_folder,"sensitivity_tests/baseline_timing/change_baseline_timing_all_results_exposed_and_non_bariatric.xlsx"))



#------------------------------------------------------------------------------------------
### OMIT COVID
#------------------------------------------------------------------------------------------

# censor all data from March 2020
df_combined_non_bariatric_no_covid <- df_combined_non_bariatric %>%
  filter(month_num <72)

pay_no_covid <- run_model_pay_pretreatment(df_combined_non_bariatric_no_covid, 
                           formula = main_model_pay,
                          save_folder = "sensitivity_tests/no_covid",
                           save_name = paste0("no_covid_effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay (overall)")

df_combined_non_bariatric_no_covid_in_work <- filter(df_combined_non_bariatric_no_covid, in_work==1)
pay_in_work_no_covid <- run_model_pay_pretreatment(df_combined_non_bariatric_no_covid_in_work, 
                          formula = main_model_pay,
                          save_folder = "sensitivity_tests/no_covid",
                           save_name = paste0("no_covid_effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay among those in work")
  

employment_no_covid <- run_model_employment_pretreatment(df_combined_non_bariatric_no_covid, 
                           formula = main_model_employment,
                          save_folder = "sensitivity_tests/no_covid",
                           save_name = paste0("no_covid_effect_on_employment_exposed_and_non_bariatric"),
                           y_lims=c(-2,8),
                            plt_title = "Probability of employment")

# combined plot
combine_plots(pay_no_covid$plot, 
              pay_in_work_no_covid$plot,                
              employment_no_covid$plot,
              save_folder = "sensitivity_tests/no_covid",
              save_name = "combined_plot_no_covid")

# compare to main model
pay_comparison <- compare_model_sig_tables(model_1_results = main_model_pay_results,
                        model_1_params = main_model_pay_params,
                        model_2_results = pay_no_covid$results,
                        model_2_params = pay_no_covid$params,
                        model_1_name = "Main_model_pay",
                        model_2_name = "Pay_no_covid",
                        save_name = "effect_on_pay_with_non_bariatric_no_covid_comparison_table",
                        save_folder = "sensitivity_tests/no_covid")

pay_in_work_comparison <- compare_model_sig_tables(model_1_results = main_model_pay_in_work_results,
                        model_1_params = main_model_pay_in_work_params,
                        model_2_results = pay_in_work_no_covid$results,
                        model_2_params = pay_in_work_no_covid$params,
                        model_1_name = "Main_model_pay_in_work",
                        model_2_name = "Pay_in_work_no_covid",
                        save_name = "effect_on_pay_in_work_with_non_bariatric_no_covid_comparison_table",
                        save_folder = "sensitivity_tests/no_covid")

employment_comparison <- compare_model_sig_tables(model_1_results = main_model_employment_results,
                        model_1_params = main_model_employment_params,
                        model_2_results = employment_no_covid$results,
                        model_2_params = employment_no_covid$params,
                        model_1_name = "Main_model_employment",
                        model_2_name = "Employment_no_covid",
                        save_name = "effect_on_employment_with_non_bariatric_no_covid_comparison_table",
                        save_folder = "sensitivity_tests/no_covid",
                        employment = T)

# combine model comparisons
comparison_table <- full_join(pay_comparison, pay_in_work_comparison, by="output")
comparison_table <- full_join(comparison_table, employment_comparison, by="output")

# combine params
all_params <- bind_rows(pay_no_covid$params, pay_in_work_no_covid$params, employment_no_covid$params)


# save out all results
data_to_save <- list('pay_overall' = pay_no_covid$results,
                      'pay_in_work' = pay_in_work_no_covid$results,
                      'employment' = employment_no_covid$results,
                      'params' = all_params,
                    'comparison' = comparison_table)

write.xlsx(data_to_save, 
             paste0(outputs_folder,"sensitivity_tests/no_covid/no_covid_all_results_exposed_and_non_bariatric.xlsx"))

 


