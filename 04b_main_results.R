# Run for exposed plus non bariatric dataset with pretreatment periods
# 3 outcomes: pay, pay in employment, probability of being in employment DONE


source("<filepath>/04a_make_analysis_dataset.R")

# formulae

main_model_pay <- "pay_deflated ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"

main_model_employment <- "in_work ~ t_treatment_6_month + ns(age_monthly, df=5, Boundary.knots=quantile(age_monthly, c(.10, .90))) | census_id_final + month_label | 0 | census_id_final"


#------------------------------------------------------------------------------------------
### MAIN MODEL WITH NON EXPOSED AND PRETRENDS
#------------------------------------------------------------------------------------------

# Headline results, using each of the datasets (exposed, and exposed with each non exposed dataset)
# Outcomes: Pay, pay for those in work, employment


# NON BARIATRIC

# PAY ALL
pay_with_non_bariatric <- run_model_pay_pretreatment(df_combined_non_bariatric, 
                           formula = main_model_pay,
                          save_folder = "main_results",
                           save_name = paste0("effect_on_pay_overall_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay (overall)")

# PAY IN WORK
df_combined_non_bariatric_in_work <- filter(df_combined_non_bariatric, in_work==1)
pay_in_work_with_non_bariatric <- run_model_pay_pretreatment(df_combined_non_bariatric_in_work, 
                          formula = main_model_pay,
                          save_folder = "main_results",
                           save_name = paste0("effect_on_pay_in_work_exposed_and_non_bariatric"),
                           y_lims=c(-250, 150),
                            plt_title = "Pay among those in work")
  
# EMPLOYMENT

# employment, without non exposed
employment_with_non_bariatric <- run_model_employment_pretreatment(df_combined_non_bariatric, 
                           formula = main_model_employment,
                          save_folder = "main_results",
                           save_name = paste0("effect_on_employment_exposed_and_non_bariatric"),
                           y_lims=c(-2,6),
                            plt_title = "Probability of employment")



combine_plots(pay_with_non_bariatric$plot, 
              pay_in_work_with_non_bariatric$plot,                
              employment_with_non_bariatric$plot,
              save_folder = "main_results",
              save_name = "non_bariatric_combined")



# combine params
all_params <- bind_rows(pay_with_non_bariatric$params, pay_in_work_with_non_bariatric$params, 
                        employment_with_non_bariatric$params)


# save out all results
data_to_save <- list('pay_overall_with_non_bariatric' = pay_with_non_bariatric$results,
                      'pay_in_work_with_non_bariatric' = pay_in_work_with_non_bariatric$results,
                      'employment_with_non_bariatric' = employment_with_non_bariatric$results,
                      'params' = all_params)

write.xlsx(data_to_save, 
             paste0(outputs_folder,"main_results/all_results_non_bariatric.xlsx"))
















