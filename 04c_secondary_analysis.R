# Use full dataset (exposed and census non bariatric)

# Compare stratification to treatment interactions using sex as a sanity check to 
# check results are similar

# Treatment interactions for the 3 main results (pay, pay among employed, employment probability):
# - sex 
# - region 
# - age at operation 
# - ethnicity 
# - IMD 
# - rural urban 

# Include births variable


source("<filepath>/04a_make_analysis_dataset.R")


# create results folders
for (i in c("census_sex", "region", "age_bands", "imd_quintile",
          "births", "ethnic_group_tb", "rural_urban")){
  dir.create(file.path(paste0(outputs_folder, "/", "secondary_analyses"), i))
}

#------------------------------------------------------------------------------------------
### MAKE IN WORK DATASET AND SET MAIN MODEL FORMULAE
#------------------------------------------------------------------------------------------


main_model_pay <- "pay_deflated ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"

main_model_employment <- "in_work ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"


# remove datasets not needed to free up memory
rm(df, df_non_bariatric)
gc()


# remove not needed columns
df_combined_non_bariatric <- df_combined_non_bariatric %>%
  select(-cal_month_fct, -cal_month, -t_treatment_6_month_baseline, -pay_winsor, -pay,
        -log_pay_plus_1, -month_num, -treatment_group, -operation_type, -month,
        -cpih_deflator_2023, -log_pay, -six_month_num)
gc()


df_combined_non_bariatric_in_work <- filter(df_combined_non_bariatric, in_work==1)


#------------------------------------------------------------------------------------------
### MAIN MODEL AS COMPARISON
#------------------------------------------------------------------------------------------

# run main model for each outcome so the models wiht treatment interactions
# can be compared using the log likelihood ratio


m_original_pay <- felm(formula(main_model_pay),
                data = df_combined_non_bariatric)

m_original_pay_in_work <- felm(formula(main_model_pay),
                data = df_combined_non_bariatric_in_work)

m_original_employment <- felm(formula(main_model_employment),
                data = df_combined_non_bariatric)


#------------------------------------------------------------------------------------------
### STRATIFIED MODEL
#------------------------------------------------------------------------------------------
# sanity check for treatment interaction estimates to check are similar

# run for sex as an example
sex_strat <- stratified_models_pay(main_model_pay, df_combined_non_bariatric, 
                      characteristic = "census_sex", 
                      y_lims = c(-220, 220), 
                      save_name = "with_non_bariatric",
                      save_folder = "secondary_analyses/census_sex/")

rm(sex_strat)


------------------------------------------------------------------------------------------
### TREATMENT INTERACTION
#------------------------------------------------------------------------------------------

treatment_interaction_variables <- c("census_sex", "region", "age_bands", "imd_quintile",
                                     "ethnic_group_tb", "rural_urban")


variable <- "ethnic_group_tb"

for (variable in treatment_interaction_variables){

  # pay
  pay <- run_model_interaction(dataset = df_combined_non_bariatric,
                         formula = main_model_pay,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_pay"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_pay)


  # pay among employed
  pay_in_work <- run_model_interaction(dataset = df_combined_non_bariatric_in_work,
                         formula = main_model_pay,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_pay_in_work"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_pay_in_work)


  # probability of employment
  employment <- run_model_interaction(dataset = df_combined_non_bariatric,
                         formula = main_model_employment,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_employment"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_employment)


  combined_results <- combine_interaction_results(df_pay = pay,
                            df_pay_in_work = pay_in_work,
                            df_employment = employment,
                            save_name = variable,
                            save_folder = paste0("secondary_analyses/", variable))

  rm(pay, pay_in_work, employment)
  gc()

}




#------------------------------------------------------------------------------------------
### TREATMENT, CALENDAR TIME AND AGE INTERACTIONS
#------------------------------------------------------------------------------------------

treatment_interaction_variables <- c("rural_urban", "census_sex", "imd_quintile", "age_bands",
                                    "ethnic_group_tb", "region")

for (variable in treatment_interaction_variables){
  
  print(variable)

  # include interaction of calendar time and age with interaction variable, compare to interaction with calendar time model
  main_model_pay_all_interactions <- paste0("pay_deflated ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90)))*", variable, "  | census_id_final + ", variable, ":month_label + month_label | 0 | census_id_final")
  main_model_employment_all_interactions <- paste0("in_work ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90)))*", variable, "  | census_id_final + ", variable, ":month_label + month_label | 0 | census_id_final")


  # pay
  pay_all_interaction <- run_model_interaction(dataset = df_combined_non_bariatric,
                         formula = main_model_pay_all_interactions,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_pay_all_interactions"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_pay)

  rm(pay_all_interaction)
  gc()

  # pay among employed
  pay_in_work_all_interaction <- run_model_interaction(dataset = df_combined_non_bariatric_in_work,
                         formula = main_model_pay_all_interactions,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_pay_in_work_all_interactions"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_pay_in_work)

  rm(pay_in_work_all_interaction)
  gc()

  # probability of employment
  employment_all_interaction <- run_model_interaction(dataset = df_combined_non_bariatric,
                         formula = main_model_employment_all_interactions,
                         save_name = paste0(variable, "_interaction_exposed_and_non_bariatric_employment_all_interactions"),
                         save_folder = paste0("secondary_analyses/", variable),
                         interaction_variable_name = variable,
                         model_to_compare = m_original_employment)

  rm(employment_all_interaction)
  gc()

  # read in to combine as not enough memory to store everything in memory and run models
  folder <- paste0(outputs_folder, "/secondary_analyses/", variable, "/")


  pay_all_interaction <- list()
  pay_all_interaction$results <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_all_interactions.csv"))
  pay_all_interaction$interaction_effects <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_all_interactions_interaction_effects.csv"))
  pay_all_interaction$lr <-  read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_all_interactions_log_likelihood_ratio.csv"))                  

  pay_in_work_all_interaction <- list()
  pay_in_work_all_interaction$results <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_in_work_all_interactions.csv"))
  pay_in_work_all_interaction$interaction_effects <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_in_work_all_interactions_interaction_effects.csv"))
  pay_in_work_all_interaction$lr <-  read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_pay_in_work_all_interactions_log_likelihood_ratio.csv"))                  

  employment_all_interaction <- list()
  employment_all_interaction$results <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_employment_all_interactions.csv"))
  employment_all_interaction$interaction_effects <- read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_employment_all_interactions_interaction_effects.csv"))
  employment_all_interaction$lr <-  read.csv(paste0(folder, variable, "_interaction_exposed_and_non_bariatric_employment_all_interactions_log_likelihood_ratio.csv"))                  


  combine_interaction_results(df_pay = pay_all_interaction,
                            df_pay_in_work = pay_in_work_all_interaction,
                            df_employment = employment_all_interaction,
                            save_name = paste0(variable, "_all_interactions"),
                            save_folder = paste0("secondary_analyses/", variable))

  rm(pay_all_interaction, pay_in_work_all_interaction, employment_all_interaction)
  gc()
  
}



#------------------------------------------------------------------------------------------
### EFFECT OF INCLUDING BIRTHS
#------------------------------------------------------------------------------------------

#### PAY ALL WITH BIRTHS WITHIN 1 YEAR AND 2-5 YEARS

main_model_pay_births <- "pay_deflated ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) + birth_in_1yr + birth_in_5yr | census_id_final + month_label | 0 | census_id_final"

pay_with_births <- run_model_pay_pretreatment(df_combined_non_bariatric, 
                           formula = main_model_pay_births,
                          save_folder = "secondary_analyses/births",
                           save_name = "effect_on_pay_with_non_bariatric_plus_births",
                           y_lims=c(-250, 150),
                           return_model = T,
                            model_to_compare = m_original_pay,
                            plt_title = "Pay (overall)")

model_1_results <- pay_with_births$results
model_1_params <- pay_with_births$params
model_2_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_overall_exposed_and_non_bariatric.csv"))
model_2_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_overall_exposed_and_non_bariatric_model_params.csv"))


pay_comparison <- compare_model_sig_tables(model_1_results = model_1_results,
                        model_1_params = model_1_params,
                        model_2_results = model_2_results,
                        model_2_params = model_2_params,
                        model_1_name = "Main_model_pay_with_births_in_past_year",
                        model_2_name = "Main_model_pay",
                        save_name = "effect_on_pay_with_non_bariatric_plus_births_comparison_table",
                        save_folder = "secondary_analyses/births")



# PAY IN WORK

pay_in_work_with_births <- run_model_pay_pretreatment(df_combined_non_bariatric_in_work, 
                           formula = main_model_pay_births,
                          save_folder = "secondary_analyses/births",
                           save_name = "effect_on_pay_in_work_with_non_bariatric_plus_births",
                           y_lims=c(-250, 150),
                           return_model = T,
                           model_to_compare = m_original_pay_in_work,
                            plt_title = "Pay among those in work")

model_1_results <- pay_in_work_with_births$results
model_1_params <- pay_in_work_with_births$params
model_2_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_in_work_exposed_and_non_bariatric.csv"))
model_2_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_pay_in_work_exposed_and_non_bariatric_model_params.csv"))


pay_in_work_comparison <- compare_model_sig_tables(model_1_results = model_1_results,
                        model_1_params = model_1_params,
                        model_2_results = model_2_results,
                        model_2_params = model_2_params,
                        model_1_name = "Main_model_pay_in_work_with_births_in_past_year",
                        model_2_name = "Main_model_pay_in_work",
                        save_name = "effect_on_pay_in_work_with_non_bariatric_plus_births_comparison_table",
                        save_folder = "secondary_analyses/births")


# EMPLOYMENT

main_model_employment_births <- "in_work ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) + birth_in_1yr + birth_in_5yr | census_id_final + month_label | 0 | census_id_final"

# pay, with non exposed obese non bariatric
employment_with_births <- run_model_employment_pretreatment(df_combined_non_bariatric, 
                           formula = main_model_employment_births,
                          save_folder = "secondary_analyses/births",
                           save_name = "effect_on_employment_in_work_with_non_bariatric_plus_births",
                           y_lims=c(-2, 6),
                           return_model = T,
                            model_to_compare = m_original_employment,
                            plt_title = "Probability of employment")


model_1_results <- employment_with_births$results
model_1_params <- employment_with_births$params
model_2_results <- read.csv(paste0(outputs_folder, "/main_results/effect_on_employment_exposed_and_non_bariatric.csv"))
model_2_params <- read.csv(paste0(outputs_folder, "/main_results/effect_on_employment_exposed_and_non_bariatric_model_params.csv"))


employment_comparison <- compare_model_sig_tables(model_1_results = model_1_results,
                        model_1_params = model_1_params,
                        model_2_results = model_2_results,
                        model_2_params = model_2_params,
                        model_1_name = "Main_model_employment_with_births_in_past_year",
                        model_2_name = "Main_model_employment",
                        save_name = "effect_on_employment_with_non_bariatric_plus_births_comparison_table",
                        save_folder = "secondary_analyses/births",
                        employment = T)


# combined plot
combine_plots(pay_with_births$plot, 
              pay_in_work_with_births$plot,                
              employment_with_births$plot,
              save_folder = "secondary_analyses/births",
              save_name = "births")

# combine all results for export

# combine params
all_params <- bind_rows(pay_with_births$params, pay_in_work_with_births$params, 
                        employment_with_births$params)

# combine likelihood ratios
all_lr <- bind_rows(pay_with_births$lr, pay_in_work_with_births$lr, 
                        employment_with_births$lr)

# combine model comparisons
comparison_table <- full_join(pay_comparison, pay_in_work_comparison, by="output")
comparison_table <- full_join(comparison_table, employment_comparison, by="output")

# save out all results
data_to_save <- list('pay_with_births' = pay_with_births$results,
                      'pay_in_work_with_births' = pay_in_work_with_births$results,
                      'employment_with_births' = employment_with_births$results,
                      'params' = all_params,
                    'likelihood ratios' = all_lr,
                    'comparison_table' = comparison_table)

write.xlsx(data_to_save, 
             paste0(outputs_folder,"secondary_analyses/births/all_results_births.xlsx"))



