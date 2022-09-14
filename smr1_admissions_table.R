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
library(janitor)       # to clean variable names

#SMRA connection
channel <- suppressWarnings(dbConnect(odbc(),  dsn='SMRA',
                                      uid=.rs.askForPassword('SMRA Username:'),
                                      pwd=.rs.askForPassword('SMRA Password:')))

odbcPreviewObject(channel, table="ANALYSIS.SMR01_PI", rowLimit=0)

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
## Starting the proper query ----
###############################################.
#location, do we want a string for this? similar for HB of treatment? specialty?
# Would UPI always be the same for a certain link_no, cis_marker? It seems like it
# but not the same for different admissions
# Variables need to be saved as upper case
smr1_admissions <- as_tibble(dbGetQuery(channel, statement=paste0(
   "SELECT distinct (link_no || '-' ||cis_marker) admission_id, 
          FIRST_VALUE(upi_number) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") UPI_NUMBER,
           MIN(admission_date) OVER (PARTITION BY link_no, cis_marker) ADMISSION_DATE,
           MAX(discharge_date) OVER (PARTITION BY link_no, cis_marker) DISCHARGE_DATE,
           FIRST_VALUE(admission_type) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") ADMISSION_TYPE,
           FIRST_VALUE(discharge_type) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") DISCHARGE_TYPE,
           SUM(length_of_stay) OVER (PARTITION BY link_no, cis_marker) LENGTH_OF_STAY,
           FIRST_VALUE(dr_postcode) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") DR_POSTCODE,
           FIRST_VALUE(sex) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") SEX,
           FIRST_VALUE(age_in_years) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") AGE_IN_YEARS,
           FIRST_VALUE(age_in_months) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") AGE_IN_MONTHS,
           FIRST_VALUE(inpatient_daycase_identifier) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") INPATIENT_DAYCASE_IDENTIFIER,
           FIRST_VALUE(location) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") LOCATION_FIRST,
          LISTAGG(location, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) LOCATION_STRING,
           FIRST_VALUE(specialty) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY ", sort_var, ") SPECIALTY_FIRST,
          LISTAGG(specialty, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) SPECIALTY_STRING,
          LISTAGG(main_condition, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) MAIN_CONDITION,
          LISTAGG(other_condition_1, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_CONDITION_1, 
          LISTAGG(other_condition_2, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_CONDITION_2, 
          LISTAGG(other_condition_3, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_CONDITION_3, 
          LISTAGG(other_condition_4, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_CONDITION_4, 
          LISTAGG(other_condition_5, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_CONDITION_5,
          LISTAGG(main_operation, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) MAIN_OPERATION,
          LISTAGG(other_operation_1, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_OPERATION_1,
          LISTAGG(other_operation_2, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_OPERATION_2,
          LISTAGG(other_operation_3, '|') WITHIN GROUP (ORDER BY ", sort_var, ") 
                  OVER (PARTITION BY link_no, cis_marker ) OTHER_OPERATION_3
       FROM ANALYSIS.SMR01_PI  
       WHERE ROWNUM <= 10000000  "))) 
 
View(smr1_admissions)

# This needs a version of odbc > 1.2.2 - I use 1.3.3
odbc::dbWriteTable(channel, "SMR1_ADM", smr1_admissions, overwrite = T)
dbListTables(channel, schema="JAMIEV01")
odbcPreviewObject(channel, table="JAMIEV01.SMR1_ADM", rowLimit=0)

# testing times on a 10 million table. the bigger the dataset the more it would take more time
# but with proper indexing this could be reduced big time
system.time(new_query <- as_tibble(dbGetQuery(channel, statement=
  "SELECT count(*), extract(year from ADMISSION_DATE) YEAR
   FROM JAMIEV01.SMR1_ADM
    WHERE ADMISSION_DATE between '1 April 1997' and '31 March 2006'
          AND regexp_like(MAIN_CONDITION, 'C5' )
  GROUP BY extract(year from ADMISSION_DATE)")) )
# 12 seconds

#This query provides slightly different results because it counts these with intermediate 
# episodes with a cancer diagnosis, but also because my table is not the whole complete smr1
system.time(old_query <- as_tibble(dbGetQuery(channel, statement=
  "SELECT count(distinct (link_no || '-' || cis_marker)), extract(year from ADMISSION_DATE) YEAR  
    FROM ANALYSIS.SMR01_PI 
    WHERE admission_date between '1 April 1997' and '31 March 2006'
          AND regexp_like(main_condition, 'C5' ) 
   GROUP BY extract(year from ADMISSION_DATE) ")) )
# 80-100 seconds 7-8 times more time

# This query provides a more similar results as the older one's logic.
system.time(new_query2 <- as_tibble(dbGetQuery(channel, statement=
 "SELECT count(*)
   FROM JAMIEV01.SMR1_ADM
    WHERE (ADMISSION_DATE between '1 April 1997' and '31 March 2006'
            OR DISCHARGE_DATE between '1 April 1997' and '31 March 2006')
          AND regexp_like(MAIN_CONDITION, 'C5' )")) )

###############################################.
# A more complex query 

alc_diag <- "E244|E512|F10|G312|G621|G721|I426|K292|K70|K852|K860|O354|P043|Q860|R780|T510|T511|T519|X45|X65|Y15|Y573|Y90|Y91|Z502|Z714|Z721"

# this takes 18.6 seconds 241 seconds
system.time(new_query_alcohol <- tbl_df(dbGetQuery(channel, statement= paste0(
  "SELECT count(*), extract(year from ADMISSION_DATE) YEAR
  FROM JAMIEV01.SMR1_ADM z
  WHERE discharge_date between  '1 April 1997' and '31 March 2006'
      and sex <> 9
      and (regexp_like(main_condition, '", alc_diag ,"')
              or regexp_like(other_condition_1,'", alc_diag ,"')
              or regexp_like(other_condition_2,'", alc_diag ,"')
              or regexp_like(other_condition_3,'", alc_diag ,"')
              or regexp_like(other_condition_4,'", alc_diag ,"')
              or regexp_like(other_condition_5,'", alc_diag ,"')) 
  GROUP BY extract(year from ADMISSION_DATE)"))))

# This takes 474 seconds
system.time(old_query_alcohol <- tbl_df(dbGetQuery(channel, statement= paste0(
  "SELECT count(distinct (link_no || '-' || cis_marker)), extract(year from ADMISSION_DATE) YEAR 
  FROM ANALYSIS.SMR01_PI z
  WHERE discharge_date between  '1 April 1997' and '31 March 2006'
      and sex <> 9
      and exists (
          select * 
          from ANALYSIS.SMR01_PI  
          where link_no=z.link_no and cis_marker=z.cis_marker
            and discharge_date between '1 April 1997' and '31 March 2006'
            and (regexp_like(main_condition, '", alc_diag ,"')
              or regexp_like(other_condition_1,'", alc_diag ,"')
              or regexp_like(other_condition_2,'", alc_diag ,"')
              or regexp_like(other_condition_3,'", alc_diag ,"')
              or regexp_like(other_condition_4,'", alc_diag ,"')
              or regexp_like(other_condition_5,'", alc_diag ,"')))
  GROUP BY extract(year from ADMISSION_DATE)"))))

###############################################.
# An even more complex query , takes 14 seconds
system.time(new_complex_query <- tbl_df(dbGetQuery(channel, statement=
    "SELECT admission_id, admission_date, discharge_date,  dr_postcode
    FROM JAMIEV01.SMR1_ADM 
    WHERE discharge_date between '1 April 1997' and '31 March 2006'
          AND regexp_like(main_condition, 'C') ")) )

# This takes 113 seconds
system.time(old_complex_query <- tbl_df(dbGetQuery(channel, statement=
    "WITH adm_table AS (
        SELECT distinct link_no || '-' || cis_marker admission_id, 
            FIRST_VALUE(hbres_currentdate) OVER (PARTITION BY link_no, cis_marker 
                ORDER BY admission_date, discharge_date) hb,
            MIN(admission_date) OVER (PARTITION BY link_no, cis_marker) start_cis,
            MAX(discharge_date) OVER (PARTITION BY link_no, cis_marker) end_cis
        FROM ANALYSIS.SMR01_PI  z
        WHERE exists(
          SELECT * 
          FROM ANALYSIS.SMR01_PI  
          WHERE link_no=z.link_no and cis_marker=z.cis_marker
              AND regexp_like(main_condition, 'C')
              AND discharge_date between '1 April 1997' and '31 March 2006'
        )
    )
    SELECT admission_id, start_cis, end_cis,  hb
    FROM adm_table 
    WHERE end_cis between '1 April 1997' and '31 March 2006' ")) )

# Delete the table once you have finished using it using dbRemoveTable
dbRemoveTable(channel, "SMR1_ADM")

###############################################.
## How the database structure should look ----
###############################################.
# One table/view at admission level
# One table/view at episode level (exists)
# One table/view at location level, so if a patient changes hospital this creates a new episode,
# but if they stay in the same location during the whole admission it would be one episode
# One table/view at specialty level. Same as above but for specialty.
# One table//view at consultant level. Same as above but for specialty



