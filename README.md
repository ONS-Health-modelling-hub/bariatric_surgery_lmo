## bariatric_surgery_lmo
code for data processing for labour market outcomes of bariatric surgery project using linked HES / PAYE (HMRC) data

# 1_data_processing 

Uses NHS Hospital episode statistics (HES) Admitted Patient Care (APC) episode level data with epiend date (admission end) between 01/04/2009 and 31/03/2021
For speed of processing, the data uses 2 input files extracted from the HES database which are all episodes prior to April 2017 (<= 31/03/2017) and all episodes post 2017 (>= 01/04/2017), filtered only to remove duplicate nhs numbers - if you have way bigger memory, section 1 of the script to identify all nhs numbers appearing in both files can be done using simple filtering from one larger file (2009 - 2021).

The script identifies any episodes with the same nhs number where operation codes 1 or 2 are in bariatric procedure codes (bariatric_opcs_lookup.csv) according to the National Obesity Audit https://digital.nhs.uk/data-and-information/clinical-audits-and-registries/national-obesity-audit

The script then removes these nhs numbers from the post 2017 processing dataframe and creates flags for all inclusion criteria for ONS study (see data_flow.png). A separate analytical dataframe which is filtered for these inclusion criteria is created - this is done so the processing dataframe can be used for wider investigation / further studies in future. 
