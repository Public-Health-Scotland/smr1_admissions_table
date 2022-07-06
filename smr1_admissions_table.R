# Creating an SMR01 table at admission/stay level rather than episode

# TODO:
# This could be moved to an SQL script once Rstudio updates
# Dealing with diagnosis strings across multiple episodes, concatenate
# What about operation dates?
# Do we want personal data there, names, etc?
# Option to do a more compressed table 
# How to check that all patient identifiers match for the same patient
# This should be an opportunity to enforce using one single variable as patient identifier
# Might keep geographies out for now as not updated to the last Scottish Postcode directory

###############################################.
## Packages ----
###############################################.
library(odbc)          # For accessing SMRA databases
library(dplyr)         # For data manipulation in the tidy way
library(magrittr)      # for more pipe operators


#SMRA connection
channel <- suppressWarnings(dbConnect(odbc(),  dsn='SMRA',
                                      uid=.rs.askForPassword('SMRA Username:'),
                                      pwd=.rs.askForPassword('SMRA Password:')))

odbcPreviewObject(channel, table="ANALYSIS.SMR01_PI", rowLimit=0)

# variables I don't need
# "EPISODE_RECORD_KEY", "RECORD_TYPE" (all the same), "SURNAME", "FIRST_FORENAME", 
# "SECOND_FORENAME", "PREVIOUS_SURNAME", "CI_CHI_NUMBER" (probably),
# "NHS_NUMBER", "HEALTH_RECORDS_SYSTEM_ID" (these two are historic),
# "ALTERNATIVE_CASE_REFERENCE", "PATIENT_IDENTIFIER" (reduce these variables), 
# "POSTCODE" (do we  need two vars for this), 
# "REFERRING_GP_GPD_GMC_NUMBER", "CARE_PACKAGE_IDENTIFIER", (historic these two)
#    "PURCHASER_CODE", "SERIAL_NUMBER", (historic)
#   "GP_REFERRAL_LETTER_NUMBER", "WAITING_LIST_GTEE_EXCEPN_CODE", (historic)
#   "CLINICAL_FACILITY_START", "CLINICAL_FACILITY_END", (not used in 10 years)
# "READY_FOR_DISCHARGE_DATE", (not used in 7 years)
# MAPPED_PROVIDER_CODE, MAPPED_PROVIDER_CODE_DATE,
# "HB_OF_RESIDENCE_CYPHER", (old geos)"HB_OF_RESIDENCE_NUMBER", "HB_OF_TREATMENT_NUMBER",
# BATCH_NUMBER, BATCH_SEQUENCE_NUMBER, DATE_RECORD_INSERTED, DATE_LAST_AMENDED, (data management)
# LENGTH_WAIT_CODE, OLD_SPECIALTY, OLD_SMR1_TADM_CODE, (historic?)
#   "DEVELOPMENT_DATA_1", "DEVELOPMENT_DATA_2", "DEVELOPMENT_DATA_3", 
#   "DEVELOPMENT_DATA_4", "DEVELOPMENT_DATA_5", "DEVELOPMENT_DATA_6", 
#   "DEVELOPMENT_DATA_7", "DEVELOPMENT_DATA_8", 
# PARL_CONSTITUENCY, CURRENT_TRUST_DMU,
# ELECTORAL_WARD, OUTPUT_AREA_2001, OUTPUT_AREA_1991, GRID_REF_EASTING,
# GRID_REF_NORTHING, DATAZONE_2001, INTZONE_2001, DATAZONE_2011,
# INTZONE_2011, OUTPUT_AREA_2011, COUNCIL_AREA_2019, SPC_2014, (geos out)
# HSCP_2019, DERIVED_CHI, DERIVED_CHI_WEIGHT, (can't have so many unique patient identifiers)
# HRG42, RECORD_DATE, ACCESSION_NO,SURNAME_SNDEX_CODE, (data management)
# PREVIOUS_SNDEX_CODE, COMMON_UNIT, SORT_MARKER, BEST_LINK_WEIGHT, (data management)
# LAST_LINKED_DATE, GLS_CIS_MARKER, ADMISSION, (data management)
# DISCHARGE, URI  (data management)

###############################################.
#varibles I am not sure about
#"SENDING_LOCATION", I think this is data management
# "DOB", 
# "MANAGEMENT_OF_PATIENT", feels very data management?
# "PROVIDER_CODE" - is this just HB/hospital location?
# DISCHARGE_TRANSFER_TO_LOCATION barely used
#CLINICIAN_MAIN_OPERATION maybe much for stay level? It will be difficult to link to the operation
# ADMISSION, DISCHARGE, URI used for sorting records only?
# GPPRAC_KEYDATE, GPPRAC_CURRENTDATE, Are these used? maybe add later
###############################################.
#Variables I need
vars_selected <- c("DR_POSTCODE, SEX, MARITAL_STATUS, 
                 ETHNIC_GROUP, GP_PRACTICE_CODE, LOCATION, SPECIALTY, 
                 SIGNIFICANT_FACILITY, CONSULTANT_HCP_RESPONSIBLE,
                 PATIENT_CATEGORY, PROVIDER_CODE, WAITING_LIST_DATE,
                 ADMISSION_DATE, WAITING_LIST_TYPE, ADMISSION_TYPE, 
                 ADMISSION_REASON, ADMISSION_TRANSFER_FROM, ADMISSION_TRANSFER_FROM_LOC,
                 DISCHARGE_DATE, DISCHARGE_TYPE,
                 DISCHARGE_TRANSFER_TO, DISCHARGE_TRANSFER_TO_LOCATION, MAIN_CONDITION,
                 OTHER_CONDITION_1, OTHER_CONDITION_2, OTHER_CONDITION_3,
                 OTHER_CONDITION_4, OTHER_CONDITION_5, MAIN_OPERATION, DATE_OF_MAIN_OPERATION,
  CLINICIAN_MAIN_OPERATION, OTHER_OPERATION_1, DATE_OF_OTHER_OPERATION_1,
  CLINICIAN_OTHER_OPERATION_1, OTHER_OPERATION_2, DATE_OF_OTHER_OPERATION_2,
  CLINICIAN_OTHER_OPERATION_2, OTHER_OPERATION_3, DATE_OF_OTHER_OPERATION_3,
  CLINICIAN_OTHER_OPERATION_3, 
                 AGE_IN_YEARS, AGE_IN_MONTHS, DAYS_WAITING, LENGTH_OF_STAY, 
                   URBAN_RURAL_CODE, INPATIENT_DAYCASE_IDENTIFIER, UPI_NUMBER,
                   HBRES_KEYDATE, HBRES_CURRENTDATE, HBTREAT_KEYDATE,
  HBTREAT_CURRENTDATE,  LINK_NO, CIS_MARKER")

###############################################.
#Variables I am starting with
vars_selected <- c("DR_POSTCODE, SEX, AGE_IN_YEARS, AGE_IN_MONTHS, LENGTH_OF_STAY, 
                 LOCATION, SPECIALTY, 
                 ADMISSION_DATE, ADMISSION_TYPE,
                 DISCHARGE_DATE, DISCHARGE_TYPE,
                 MAIN_CONDITION,
                 OTHER_CONDITION_1, OTHER_CONDITION_2, OTHER_CONDITION_3,
                 OTHER_CONDITION_4, OTHER_CONDITION_5, MAIN_OPERATION,
                 OTHER_OPERATION_1, OTHER_OPERATION_2, OTHER_OPERATION_3,
                 INPATIENT_DAYCASE_IDENTIFIER, UPI_NUMBER,
                 LINK_NO, CIS_MARKER")

# Sorting variables
sort_var <- "link_no, admission_date, discharge_date, admission, discharge, uri"

# Testing if a variable is used and when
tests <- as_tibble(dbGetQuery(channel, statement=
  "SELECT count(DISCHARGE_TRANSFER_TO_LOCATION),  extract(year from admission_date)
   FROM ANALYSIS.SMR01_PI 
  WHERE DISCHARGE_TRANSFER_TO_LOCATION is not null
   GRoup by extract(year from admission_date) ")) 

tests <- as_tibble(dbGetQuery(channel, statement=paste0(
  "SELECT ", vars_selected,
  " FROM ANALYSIS.SMR01_PI 
   WHERE ROWNUM <= 100000 
   ORDER BY ", sort_var))) %>% 
  janitor::clean_names()

tests <- tests %>% group_by(link_no, cis_marker) %>% 
  mutate(count = n()) %>% ungroup()


###############################################.
## Starting the proper query ----
###############################################.

smr1_admissions <- as_tibble(dbGetQuery(channel, statement=paste0(
   "SELECT distinct link_no, cis_marker, 
           MIN(admission_date) OVER (PARTITION BY link_no, cis_marker
                                     ORDER BY ", sort_var, ") admission_date,
           MAX(discharge_date) OVER (PARTITION BY link_no, cis_marker
                                     ORDER BY ", sort_var, ") discharge_date
       FROM ANALYSIS.SMR01_PI  
       WHERE ROWNUM <= 100000 "))) %>% 
  clean_names() #variables to lower case

dbWriteTable(channel, "smr1_adm", smr1_admissions)

# Delete the table once you have finished using it using dbRemoveTable
dbRemoveTable(channel, "smr1_adm")


