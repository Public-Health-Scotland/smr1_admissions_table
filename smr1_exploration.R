# Exploration of the SMR01 dataset

###############################################.
## Packages ----
###############################################.
library(odbc)          # For accessing SMRA databases
library(dplyr)         # For data manipulation in the tidy way
library(magrittr)      # for more pipe operators
library(janitor)       # to clean variable names

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

#Testing if UPI number is constant for an admission
tests <- as_tibble(dbGetQuery(channel, statement=
                                "SELECT link_no, count(distinct UPI_NUMBEr)
                              FROM ANALYSIS.SMR01_PI 
                              GRoup by link_no HAVING COUNT(link_no)>1 ")) 

#Testing if link_no is constant for an admission
tests <- as_tibble(dbGetQuery(channel, statement=
 "SELECT UPI_NUMBEr, count(distinct link_no)
 FROM ANALYSIS.SMR01_PI 
 GRoup by UPI_NUMBEr HAVING COUNT(UPI_NUMBEr)>1 ")) 

tests2 <- as_tibble(dbGetQuery(channel, statement=paste0(
"SELECT link_no, cis_marker, UPI_NUMBEr, age_in_years, dob,
first_forename || ' ' ||surname name, admission_date
FROM ANALYSIS.SMR01_PI 
WHERE link_no = ", linknotest))) 

tests4 <- as_tibble(dbGetQuery(channel, statement=paste0(
  "SELECT link_no, cis_marker, UPI_NUMBEr, age_in_years, dob,
  first_forename || ' ' ||surname name, admission_date
  FROM ANALYSIS.SMR01_PI 
  WHERE UPI_NUMBEr = ", upinotest))) 


# This works to aggregate conditions, doesn't work with distinct and struggles with order by
tests3 <- as_tibble(dbGetQuery(channel, statement=paste0(
  "SELECT distinct link_no, cis_marker, 
  LISTAGG(main_condition, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
  OVER (PARTITION BY link_no, cis_marker ) diag
  FROM ANALYSIS.SMR01_PI 
  WHERE ROWNUM <= 100000 "))) %>% 
  janitor::clean_names()

tests3 <- tests3 %>% mutate(lengthchar =nchar(diag))