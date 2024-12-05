# add a column where the pay column has been deflated
add_deflated_pay <- function(df, pay_col){
  # deflated pay
  deflation_lookup <- read.csv(<filepath>)

  deflation_lookup <- deflation_lookup %>%
    mutate(month = as.Date(month, format='%Y-%m-%d'))
  
  df <- df %>%
    mutate(month = as.Date(paste(month_label, "-01", sep="")))

  df <- left_join(df, select(deflation_lookup, -month_org), by = 'month', copy=TRUE)

  df <- df %>%
    mutate(pay_deflated = {{pay_col}}/cpih_deflator_2023)
  
  return(df)
}


treatment_variables <- function(df){
  # create treatment variables
  df <- df %>% 
      mutate(cal_month_fct = as.factor(substr(month_label, 6, nchar(month_label))),
            month = as.Date(paste(month_label, "-01", sep="")),
             cal_month = month(month),
             month_label = as.factor(month_label),
            t_treatment_year = ceiling(t_op/12),
            t_treatment_6_month =case_when(t_op <= 0 ~ ceiling(t_op/6),
                                           t_op > 0 & t_op <= 6 ~ t_op,
                                           t_op > 6 ~ ceiling(t_op/6) + 5)) %>%
      mutate(t_treatment_year_baseline = as.factor(ifelse(t_treatment_year>0, t_treatment_year, 0)),
            t_treatment_6_month_baseline = as.factor(ifelse(t_treatment_6_month>0, t_treatment_6_month,0)),
            t_treatment_6_month = as.factor(t_treatment_6_month),
            t_treatment_year = as.factor(t_treatment_year))
  
  df$t_treatment_6_month <- relevel(df$t_treatment_6_month, "0")

  return(df)
}


# pay: unadjusted pay
# pay_winsor: winsorised pay
# pay_deflated: deflated winsorised pay
# log_pay: log of deflated pay (not winsorised)
# log_pay_plus_1: log(deflated_pay+1) (not winsorised)
outcome_variables <- function(df){
  
  # create outcome variables
  q = quantile(df$pay, prob=c(0.0001, 0.9999))
  df <- df %>% 
    mutate(in_work =ifelse(pay > 0, 1, 0),
         pay_winsor = ifelse(pay > q[[2]], q[[2]],pay))

  df <- add_deflated_pay(df, pay_winsor)
  
  df <- df %>%
    mutate(log_pay = log(pay_deflated),
          log_pay_plus_1 = log(pay_deflated + 1))
  
  return(df)
}


set_factors <- function(dataframe){
  dataframe$census_sex <- as.factor(dataframe$census_sex)
  dataframe$census_sex <- relevel(dataframe$census_sex, "Females")
  
  dataframe$region <- as.factor(dataframe$region)
  dataframe$region <- relevel(dataframe$region, "London")
  
  dataframe$imd_quintile <- as.factor(dataframe$imd_quintile)
  dataframe$imd_quintile <- relevel(dataframe$imd_quintile, "1")
  
  dataframe$age_bands <- as.factor(dataframe$age_bands)
  dataframe$age_bands <- relevel(dataframe$age_bands, "25-34")
  
  dataframe$rural_urban <- as.factor(dataframe$rural_urban)
  dataframe$rural_urban <- relevel(dataframe$rural_urban, "Urban")
  
  dataframe$month_label <- as.factor(dataframe$month_label)
  dataframe$month_label <- relevel(dataframe$month_label, "2014-4")
  
  dataframe$ethnic_group_tb <- as.factor(dataframe$ethnic_group_tb)
  dataframe$ethnic_group_tb <- relevel(dataframe$ethnic_group_tb, "White")
  
  return(dataframe)
}


# plot post event data (pre event data is all baseline)
plot_post_event <- function(results, y_lim=NA, y_label=NA, save_name=NA,
                            colour=NA, estimate_col=NA,
                           conf.high_col=NA, conf.low_col=NA, plt_title = NA){

  # get left and right values for bar plots and mid point for error bars
  results <- results %>%
    mutate(t= as.numeric(gsub("t_treatment_6_month_baseline", "", term)),
        months_since_operation_left = ifelse(t<=6, t-1, t*6 - 36),
        months_since_operation_right = ifelse(t<=6, t, t*6 - 30),
        months_since_operation_mid = ifelse(t<=6, t-0.5, t*6 - 33))
  
  if (!is.na(estimate_col)){
    results_plot <- results
    results_plot$estimate <- results[[estimate_col]]
    results_plot$conf.low <- results[[conf.low_col]]
    results_plot$conf.high <- results[[conf.high_col]]
  }else{results_plot <- results}
  
  if (is.na(y_label)){
    y_label = "Effect"
  }
  if (is.na(plt_title)){
    plt_title = ""
  }
  if (is.na(colour)){
    colour="#00aaff"
  }
  
  # plot
  plot <- ggplot(data = results_plot, aes(ymin=0)) + 
    geom_rect(aes(xmin = months_since_operation_left, 
                xmax = months_since_operation_right, ymax = estimate),
            fill=colour, color="black") +
    geom_errorbar(aes(x=months_since_operation_mid, ymin=conf.low, ymax=conf.high), width=1) +
    geom_hline(yintercept=0, linetype="dotted")+
    scale_x_continuous(breaks = seq(0, 100, by = 6)) +
    labs(title = plt_title, x="Months since operation", y="Effect on monthly income (£)")+
    theme_bw()
 
  if (length(y_lim)==2){
    plot <- plot + ylim(y_lim[1],y_lim[2])
    }
  
  if (!is.na(save_name)){
    ggsave(paste0(outputs_folder,save_name, ".png"), width = 8, height = 5, units = "in")
    # save out dataset
    write.csv(results, paste0(outputs_folder,save_name, ".csv"), row.names=FALSE)
  }
  
  return(plot)
}




# plot post event data (pre event data is all baseline)
plot_with_pretreatment <- function(results, y_lim=NA, y_label=NA, save_name=NA,
                            colour=NA, estimate_col=NA,
                           conf.high_col=NA, conf.low_col=NA,
                                  interaction = FALSE, plt_title = NA){
  
  
  # get left and right values for bar plots and mid point for error bars
  results <- results %>%
    mutate(t= as.numeric(gsub("t_treatment_6_month", "", term)),
        months_since_operation_left = case_when(t>0 & t<=6 ~ t-1, 
                                                t>6 ~ t*6 - 36,
                                               t<=0 ~ t*6 - 6),
        months_since_operation_right = case_when(t>0 & t<=6 ~ t, 
                                                t>6 ~ t*6 - 30,
                                               t<=0 ~ t*6),
        months_since_operation_mid = case_when(t>0 & t<=6 ~ t-0.5, 
                                                t>6 ~ t*6 - 33,
                                               t<=0 ~ t*6 -3))

  
  if (!is.na(estimate_col)){
    results_plot <- results
    results_plot$estimate <- results[[estimate_col]]
    results_plot$conf.low <- results[[conf.low_col]]
    results_plot$conf.high <- results[[conf.high_col]]
  }else{results_plot <- results}
  
  print(results_plot)
  
  
  if (is.na(y_label)){
    y_label = "Effect"
  }
  if (is.na(plt_title)){
    plt_title = ""
  }
    if (is.na(colour)){
    colour="#00aaff"
  }
  
  results_plot <- filter(results_plot, grepl("t_treatment_6_month", term))
  
  # plot
  plot <- ggplot(data = results_plot, aes(ymin=0)) + 
    geom_rect(aes(xmin = months_since_operation_left, 
                xmax = months_since_operation_right, ymax = estimate),
            fill=colour, color="black") +
    geom_errorbar(aes(x=months_since_operation_mid, ymin=conf.low, ymax=conf.high), width=1) +
    geom_hline(yintercept=0, linetype="dotted")+
    scale_x_continuous(breaks = seq(-120, 100, by = 6)) +
    theme_bw()
  
  if (interaction == TRUE){
    n_interactions <- nrow(distinct(results_plot, interaction_variable))
    plot <- plot + facet_wrap(vars(interaction_variable), nrow = n_interactions)
  }else(n_interactions<-1)
 
  if (!is.na(y_lim[1])){
    plot <- plot + ylim(y_lim[1],y_lim[2])
    }
  
  if (!is.na(y_label)){
    plot <- plot + labs(title = plt_title, x="Months since operation", y=y_label)
    }else{plot <- plot + labs(x="Months since operation", y="Effect on monthly income (£)")}
  
  if (!is.na(save_name)){
    ggsave(paste0(outputs_folder,save_name, ".png"), width = 12, height = 4*n_interactions, units = "in")
    ggsave(paste0(outputs_folder,save_name, ".pdf"), width = 12, height = 4*n_interactions, units = "in")
  }
  
  return(plot)
}


model_params <- function(model, save_name = NA, model_name = NA){
  
  if (!is.na(model_name)){
    model_params <- data.frame(model = model_name,
                               AIC = as.character(round(AIC(model),0)),
                              BIC = as.character(round(BIC(model),0)))
  }else{model_params <- data.frame(AIC = as.character(round(AIC(model),0)),
                              BIC = as.character(round(BIC(model),0)))
       }
    
  if (!is.na(save_name)){
    write.csv(model_params, paste0(outputs_folder,save_name, ".csv"), row.names=FALSE)
  }
  
  return(model_params)
}


run_model_pay_postevent <- function(dataset, formula, save_name, save_folder,
                                       y_lims=NA, log=F, plt_title = NA){

  m <- felm(formula(formula),
                data = dataset)

  m_results <- broom::tidy(m, conf.int=T) 

  
  if (log==T){
    m_results <- m_results %>%
      mutate(exp_estimate = exp(estimate),
         exp_conf_low = exp(conf.low),
         exp_conf_high = exp(conf.high)) %>%
      mutate(percent_change_estimate = (exp_estimate-1)*100,
        percent_change_conf_low = (exp_conf_low-1)*100,
        percent_change_conf_high = (exp_conf_high-1)*100)
  }
  
  print(m_results)
  
  write.csv(m_results, paste0(outputs_folder,save_folder, "/", save_name, ".csv"), row.names=FALSE)
  
  model_params <- model_params(m, model_name = save_name)  
  print(model_params)

  if (log==T){
    plot <- plot_post_event(m_results, y_lim=y_lims, 
                       y_label="Effect on log monthly income (£)",
               save_name = paste0(save_folder, "/", save_name), 
                                   estimate_col="percent_change_estimate",
                           conf.high_col="percent_change_conf_low", conf.low_col="percent_change_conf_high",
                           plt_title = plt_title)
  }else{plot <- plot_post_event(m_results, y_lim=y_lims, 
                       y_label="Effect on monthly income (£)",
               save_name = paste0(save_folder, "/", save_name),
                           plt_title = plt_title)}
  
  print(plot)
  
  return(list(results = m_results, params = model_params, plot = plot))

}



run_model_pay_pretreatment <- function(dataset, formula, save_name, save_folder,
                                       y_lims=NA, log=F, plt_title=NA, return_model=F,
                                      model_to_compare = NA){

  m <- felm(formula(formula),
                data = dataset)

  m_results <- broom::tidy(m, conf.int=T) 

  
  if (log==T){
    m_results <- m_results %>%
      mutate(exp_estimate = exp(estimate),
         exp_conf_low = exp(conf.low),
         exp_conf_high = exp(conf.high)) %>%
      mutate(percent_change_estimate = (exp_estimate-1)*100,
        percent_change_conf_low = (exp_conf_low-1)*100,
        percent_change_conf_high = (exp_conf_high-1)*100)
  }
  
  print(m_results)
  
  write.csv(m_results, paste0(outputs_folder,save_folder, "/", save_name, ".csv"), row.names=FALSE)
  
  model_params <- model_params(m, model_name = save_name)  
  print(model_params)
  write.csv(model_params, paste0(outputs_folder,save_folder, "/", save_name, "_model_params.csv"), row.names=FALSE)

  if (log==T){
    plot <- plot_with_pretreatment(m_results, y_lim=y_lims, 
                       y_label="Effect on log monthly income (£)",
               save_name = paste0(save_folder, "/", save_name), 
                                   estimate_col="percent_change_estimate",
                           conf.high_col="percent_change_conf_low", conf.low_col="percent_change_conf_high",
                                  plt_title = plt_title)
  }else{plot <- plot_with_pretreatment(m_results, y_lim=y_lims, 
                       y_label="Effect on monthly income (£)",
               save_name = paste0(save_folder, "/", save_name),
                                  plt_title = plt_title)}
  
  print(plot)
  
  if (!is.na(model_to_compare)){
    lr <- data.frame(lrtest(m, model_to_compare))
    print(lr)
    lr$model_name <- save_name
    write.csv(lr, paste0(outputs_folder,save_folder, "/", save_name, "_log_likelihood_ratio.csv"), row.names=FALSE)
  }else{lr = NA}
  
  if (return_model==T){return_list = list(results = m_results, params = model_params, plot = plot,
                                         lr = lr,  m = m)
    }else{return_list = list(results = m_results, params = model_params, plot = plot, lr = lr)}
  
  return(return_list)

}



run_model_employment_pretreatment <- function(dataset, formula, save_name, save_folder,
                                       y_lims=c(-4,10), plt_title = NA, return_model = F,
                                      model_to_compare = NA){

  m <- felm(formula(formula),
                data = dataset)

  m_results <- broom::tidy(m, conf.int=T) 
  m_results <- m_results %>%
    mutate(estimate_perc = estimate*100,
        conf.low_perc = conf.low*100,
        conf.high_perc = conf.high*100)
  print(m_results)
  
  write.csv(m_results, paste0(outputs_folder,save_folder, "/", save_name, ".csv"), row.names=FALSE)
  
  model_params <- model_params(m, model_name = save_name)
  print(model_params)
  write.csv(model_params, paste0(outputs_folder,save_folder, "/", save_name, "_model_params.csv"), row.names=FALSE)

  
  plot <- plot_with_pretreatment(m_results, y_lim=y_lims, 
                       y_label="Effect on probability of being in work (%)",
               save_name = paste0(save_folder, "/", save_name),
                                colour="#8b0850", estimate_col="estimate_perc",
                           conf.high_col="conf.low_perc", conf.low_col="conf.high_perc",
                                plt_title = plt_title)
  print(plot)
  
  if (!is.na(model_to_compare)){
    lr <- data.frame(lrtest(m, model_to_compare))
    print(lr)
    lr$model_name <- save_name
    write.csv(lr, paste0(outputs_folder,save_folder, "/", save_name, "_log_likelihood_ratio.csv"), row.names=FALSE)
  }else{lr = NA}
  
  if (return_model==T){return_list = list(results = m_results, params = model_params, plot = plot,
                                         lr = lr,  m = m)
    }else{return_list = list(results = m_results, params = model_params, plot = plot, lr = lr)}
  
  return(return_list)

}


run_model_employment_postevent <- function(dataset, formula, save_name, save_folder,
                                       y_lims=c(-4,10), plt_title = NA){

  m <- felm(formula(formula),
                data = dataset)

  m_results <- broom::tidy(m, conf.int=T) 
  m_results <- m_results %>%
    mutate(estimate_perc = estimate*100,
        conf.low_perc = conf.low*100,
        conf.high_perc = conf.high*100)
  print(m_results)
  
  write.csv(m_results, paste0(outputs_folder,save_folder, "/", save_name, ".csv"), row.names=FALSE)
  
  model_params <- model_params(m, model_name = save_name)
  print(model_params)
  
  plot <- plot_post_event(m_results, y_lim=y_lims, 
                       y_label="Effect on probability of being in work (%)",
               save_name = paste0(save_folder, "/", save_name),
                                colour="#8b0850", estimate_col="estimate_perc",
                           conf.high_col="conf.low_perc", conf.low_col="conf.high_perc",
                         plt_title = plt_title)
  print(plot)
  
  return(list(results = m_results, params = model_params, plot = plot))

}



significance_table <- function(results, params, employment=F){
  if (employment == T){   
    results <- results %>%
      mutate(significance = case_when(p.value<0.001 ~ "***",
                                 p.value<0.01 ~ "**",
                                 p.value < 0.05 ~ "*",
                                 TRUE ~ ""),
             output = term) %>%
      mutate(value = paste0(as.character(sprintf("%0.1f",estimate*100)), significance, " (", 
                            sprintf("%0.1f",conf.low*100), ", ", sprintf("%0.1f",conf.high*100), ")")) %>%
      select(output, value)
  }else{   
    results <- results %>%
      mutate(significance = case_when(p.value<0.001 ~ "***",
                                 p.value<0.01 ~ "**",
                                 p.value < 0.05 ~ "*",
                                 TRUE ~ ""),
             output = term) %>%
      mutate(value = paste0(as.character(sprintf("%0.1f",estimate)), significance, " (", 
                            sprintf("%0.1f",conf.low), ", ", sprintf("%0.1f",conf.high), ")")) %>%
      select(output, value)
  }

  params <- params %>% 
      pivot_longer(cols = c("AIC", "BIC"), names_to = "output") %>%
      mutate(value = as.character(value)) %>%
      select(-model)

  sig_table <- bind_rows(results, params)
  
  return(sig_table)
}

compare_model_sig_tables <- function(model_1_results, model_1_params,
                                     model_2_results, model_2_params,
                                     model_1_name, model_2_name,
                                    save_name = NA,
                                    save_folder = NA,
                                    employment = F){

  sig_table_1 <- significance_table(model_1_results, model_1_params, employment) %>%
     rename({{model_1_name}} := value)
  
  sig_table_2 <- significance_table(model_2_results, model_2_params, employment) %>%
     rename({{model_2_name}} := value)
  
  sig_table_combined <- full_join(sig_table_1, sig_table_2,
                             by = "output") %>%
                        data.frame()
  
  if (!is.na(save_name)){
    write.csv(sig_table_combined, paste0(outputs_folder, save_folder, "/", save_name, ".csv"),
             row.names=FALSE)
  }
  
  return(sig_table_combined)
  
}



stratified_models_pay <- function(formula, dataset, characteristic, values=NA, y_lims=NA, save_name,
                                 save_folder = "secondary_analyses"){
  
  if (is.na(values)){
    values = dataset %>% select({{characteristic}}) %>% distinct() %>% pull()
    print(values)
  }
  
  for(value in values){
    
    # filtered dataset
    df_filtered <- filter(dataset, get({{characteristic}}) == value)
    
    # run model
    model <- run_model_pay_pretreatment(df_filtered, 
                           formula = formula,
                          save_folder = save_folder,
                           save_name = paste0("pay_", save_name, "_", characteristic, "_", value),
                           y_lims=y_lims)

  }

}



# calculate treatment effects for each treatment group then aggregate
did <- function(y_col, dataset, treatment_group_col = "treatment_group", 
                 cal_time_col = "six_month_num", id_col = "id",
                 covariate = NA,
                save_name = NA, save_folder = NA){
  
  if (!is.na(covariate)){
    xformula=formula(paste0("~+ ", covariate))
  }else{xformula = formula("~1")}
  
  # run did model
  did_with_non_bariatric <- att_gt(yname = y_col,
                                   tname = cal_time_col,
                                   idname = id_col,
                                   gname = treatment_group_col,
                                   xformla = xformula,
                                   data = dataset,
                                   control_group = "notyettreated", # use not yet treated as well as never treated as controls
                                   allow_unbalanced_panel = TRUE, # include people who di not have measurements in all time periods
                                   base_period = "universal", # for the pretrend period, compare each time period with the baseline period (not the period before it)
                                   print_details = TRUE)

  print(summary(did_with_non_bariatric))
  
  # plot the results
#  plt <- ggdid(did_with_non_bariatric)
  
  results <- tidy(did_with_non_bariatric)
  results$event_time <- results$time - results$group
  
  if (!is.na(save_name)){
    write.csv(results, paste0(outputs_folder, save_folder, "/treatment_group_effects_", save_name, ".csv"), row.names=FALSE)
#    ggsave(plt, paste0(outputs_folder,save_folder, "/treatment_group_effects_", save_name, ".png"), width = 8, height = 5, units = "in")
  }
  
  # aggregate with dynamic treatment effects
  agg.es <- aggte(did_with_non_bariatric, type = "dynamic", na.rm=TRUE)
  print(summary(agg.es))
  agg_results <- tidy(agg.es)

#  agg_plt <- ggdid(agg.es)
#  print(agg_plt)
  
  if (!is.na(save_name)){
    write.csv(agg_results, paste0(outputs_folder, save_folder, "/agg_treatment_group_effects_", save_name, ".csv"), row.names=FALSE)
#    ggsave(agg_plt, paste0(outputs_folder,save_folder, "/agg_treatment_group_effects_", save_name, ".png"), 
#           width = 8, height = 5, units = "in", device = "png")
  }
  
  return(list("results" = results, "agg_results" = agg_results))
}



make_dataset_for_did <- function(dataset){
      # make data for aggregated treatment effects package

    # group pay to use use 6 monthly calendar time
    # include average pay over the 6 months
    # include factor to indicate tim eperiod
    # include age at start of the 6 months
    dataset_for_did <- dataset %>%
      group_by(census_id_final, six_month_num) %>%
      mutate(pay_deflated_six_month = mean(pay_deflated),
            age_monthly_six_month = min(age_monthly)) %>%
      mutate(in_work_six_month = ifelse(pay_deflated_six_month>0, 1, 0)) %>%
      ungroup() %>%
    # for original model to do with 6 month variables
      mutate(event_time = as.factor(ifelse(treat==1, six_month_num - treatment_group + 1, 0)),
             cal_period = as.factor(six_month_num)) %>%
      select(treat, six_month_num, census_id_final, treatment_group, 
             pay_deflated_six_month, age_monthly_six_month, cal_period, event_time,
            in_work_six_month, census_sex) %>%   
      distinct() %>%
      data.frame()

    # create numeric id column
    dataset_for_did <- dataset_for_did %>%
      group_by(census_id_final) %>%
      mutate(id = cur_group_id()) %>%
      ungroup()
    
    return(dataset_for_did)

  }


# calculate the treatment effects for the reference category and for
# each other value of the interaction term
# input results from broom::tidy(m)
# outputs results in the same form, the estimate for each grouping now includes the treatment interaction,
# and has a column to denote the group of the interaction term (e.g. Male and Female for sex)

# currently it does not join on the interactions for the spline
# terms due to the special characters (they are lost in the pivot longer)
interaction_treatment_effects <- function(model_results, interaction_reference = NA,
                                         interaction_variable_name = NA){

  model_results_tidy <- broom::tidy(model_results, conf.int=T) 
  
  # extract interaction terms and join on to calculate moderated treatment effects
  interaction_terms <- model_results_tidy %>%
    filter(grepl(":", term)) %>%
    rename(interaction_term = term) %>%
    mutate(interaction_variable = sub(".*:", "", interaction_term),
           term = sub(":.*", "", interaction_term)) %>%
    rename(interaction_estimate = estimate,
          interaction_std.error = std.error) %>%
    select(term, interaction_term, interaction_variable,
           interaction_estimate, interaction_std.error)
  
  if (!is.na(interaction_variable_name)){
    interaction_terms <- interaction_terms %>%
      mutate(interaction_variable = sub(interaction_variable_name, "", interaction_variable))
  }

  # extract estimates for the reference category
  m_results_non_interaction_terms <- model_results_tidy %>%
    filter(!grepl(":", term)) %>%
    select(term, estimate, std.error)

  # join interaction estimates onto reference estimates
  moderated_treatment_effects <- left_join(interaction_terms, m_results_non_interaction_terms,
                                          by = "term")

  # calculate estimates for the non reference categories
  moderated_treatment_effects <- moderated_treatment_effects %>%
    mutate(estimate = estimate + interaction_estimate) %>%
    select(-interaction_estimate)

  # remove special characters
  # change names to remove special characters to be able to join
  # onto covariance
  moderated_treatment_effects <- moderated_treatment_effects %>%
    mutate(interaction_term = gsub(":", ".", interaction_term),
          interaction_term = gsub("-", ".", interaction_term),
          interaction_term = gsub(" ", ".", interaction_term))

  # get values of covariance from covariance matrix
  vcov=vcov(model_results)
  vcov_df <- data.frame(vcov)
  vcov_df$term1 = rownames(vcov)
  vcov_df <- pivot_longer(vcov_df, !term1, names_to = "term2", values_to = "covariance") %>%
    data.frame()

  # join on covariance values
  moderated_treatment_effects <- left_join(moderated_treatment_effects, vcov_df, 
                                           by=c("term" = "term1", "interaction_term"="term2"))

  # calculate standard error
  moderated_treatment_effects <- moderated_treatment_effects %>%
    mutate(std.error = sqrt(std.error^2 + interaction_std.error^2 + 2*covariance)) %>%
    select(-interaction_std.error, -covariance, -interaction_term)


  # label for the reference category
  if (!is.na(interaction_reference)){
      m_results_non_interaction_terms$interaction_variable <- interaction_reference
  }else{m_results_non_interaction_terms$interaction_variable <- "Reference"}


  # create dataset of estimates for reference and non reference categories
  all_results <- bind_rows(m_results_non_interaction_terms, 
                           moderated_treatment_effects)

  # calculate confidence limits, statistic and p.value
  all_results <- all_results %>%
    mutate(conf.low = estimate - qnorm(0.975)*std.error,
           conf.high = estimate + qnorm(0.975)*std.error,
          statistic = estimate/std.error,
          p.value = pnorm(-abs(estimate) / std.error) * 2)

  # order columns to match summary output
  all_results <- all_results %>%
    select(term, interaction_variable, estimate, std.error, 
           statistic, p.value, conf.low, conf.high) %>%
    data.frame()
  
  return(all_results)
}

# wrapping function to run the model wiht interactions and save and plot results
run_model_interaction <- function(dataset, formula, save_name, save_folder,
                                      y_lims=NA,
                                      interaction_variable_name = NA,
                                      model_to_compare = NA,
                                     percent = F,
                                      return_model = F){
  
  interaction_formula <- gsub("t_treatment_6_month", 
                            paste0("t_treatment_6_month", "*", interaction_variable_name),
                            formula)
  
  print(interaction_formula)

  interaction_reference <- levels(dataset[[interaction_variable_name]])[1]

  m <- felm(formula(interaction_formula),
                data = dataset)
  
  if (!is.na(model_to_compare)){
    lr <- data.frame(lrtest(m, model_to_compare))
    print(lr)
    write.csv(lr, paste0(outputs_folder,save_folder, "/", save_name, "_log_likelihood_ratio.csv"), row.names=FALSE)
  }else{lr = NA}

  m_results <- broom::tidy(m, conf.int=T) 
  
  if (percent == T){
    m_results <- m_results %>%
      mutate(estimate_perc = estimate*100,
        conf.low_perc = conf.low*100,
        conf.high_perc = conf.high*100)
  }
  
  print(m_results)
  write.csv(m_results, paste0(outputs_folder,save_folder, "/", save_name, ".csv"), row.names=FALSE)
  
  # calulcate values of treatemnt effects for all values of the interaction term
  m_results_interactions <- interaction_treatment_effects(m,
                         interaction_reference = interaction_reference,
                         interaction_variable_name = interaction_variable_name) 
  
  if (percent == T){
    m_results_interactions <- m_results_interactions %>%
      mutate(estimate_perc = estimate*100,
        conf.low_perc = conf.low*100,
        conf.high_perc = conf.high*100)
  }
  
  print(m_results_interactions)
  write.csv(m_results_interactions, paste0(outputs_folder,save_folder, "/", save_name, "_interaction_effects.csv"), row.names=FALSE)


  if (percent == T){
  plot <- plot_with_pretreatment(m_results_interactions, 
                         y_lim=y_lims, 
                         y_label="Effect on probability of being in work (%)",
                         save_name = paste0(save_folder, "/", save_name),
                          colour="#8b0850", estimate_col="estimate_perc",
                           conf.high_col="conf.low_perc", conf.low_col="conf.high_perc", 
                         interaction=TRUE)
  }else{plot <- plot_with_pretreatment(m_results_interactions, 
                         y_lim=y_lims, 
                         y_label="Effect on monthly income (£)",
                         save_name = paste0(save_folder, "/", save_name), 
                         interaction=TRUE)}
  print(plot)
  
#  if (return_model==T){return_list = list(results = m_results, interaction_effects = m_results_interactions,
#                                          plot = plot, lr = lr, m = m)
#    }else{return_list = list(results = m_results, interaction_effects = m_results_interactions,
#                                          plot = plot, lr = lr)}
  
  return(list(results = m_results, interaction_effects = m_results_interactions,
                                          plot = plot, lr = lr))

}
  
  

combine_interaction_results <- function(df_pay, df_pay_in_work, df_employment,
                                       save_name, save_folder){

  df_pay$lr$model <- "pay"
  df_pay_in_work$lr$model <- "pay_in_work"
  df_employment$lr$model <- "employment"

  # combine params
  all_lr <- bind_rows(df_pay$lr, df_pay_in_work$lr, df_employment$lr)

  # combine results and interaction effects
  data_to_save <- list('pay_overall_results' = df_pay$results,
                        'pay_in_work_results' = df_pay_in_work$results,
                        'employment_results' = df_employment$results,
                       'pay_overall_interaction_effects' = df_pay$interaction_effects,
                        'pay_in_work_interaction_effects' = df_pay_in_work$interaction_effects,
                        'employment_interaction_effects' = df_employment$interaction_effects,
                        'lr' = all_lr)

  write.xlsx(data_to_save, 
               paste0(outputs_folder,save_folder, "/", save_name, ".xlsx"))
}

  
  
combine_plots <- function(pay_plt, pay_in_work_plt, employment, save_folder, save_name){
  combined_plts <- arrangeGrob(pay_plt, pay_in_work_plt, employment, 
                          nrow = 3, heights = c(1, 1, 1))

  ggsave(file=paste0(outputs_folder,save_folder, "/", save_name, ".png"), 
       combined_plts, width = 10, height = 10, units = "in")
  
}