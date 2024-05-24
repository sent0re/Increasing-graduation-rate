-- These query were used to transfer CSV files from Azure containers to Snowflake warehouse
CREATE or REPLACE WAREHOUSE EDUCATION_INSIGHTS;


CREATE OR REPLACE DATABASE STUDENTS_DB;

USE STUDENTS_DB;


CREATE OR REPLACE NOTIFICATION INTEGRATION education_data_event
ENABLED= TRUE
TYPE= QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI= 'https://educationinsights.queue.core.windows.net/education-data-queue'
AZURE_TENANT_ID= '5b051ea7-7a73-40c4-b43a-7d7ac919f044';

SHOW INTEGRATIONS;

desc NOTIFICATION INTEGRATION EDUCATION_DATA_EVENT;



-- Admissions stage
--------------------------------------------
============================================

CREATE OR REPLACE STAGE admissions_stage
url='azure://educationinsights.blob.core.windows.net/education-data-blob/admissions.csv'
Credentials = (azure_sas_token='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwlacupiytfx&se=2024-06-19T17:24:30Z&st=2024-05-19T09:24:30Z&spr=https&sig=j%2F%2BxbKZZOWfdZiCysf1BV3p1MWQYLb9w4jRJEjlk1Hg%3D');


CREATE OR REPLACE TABLE admissions(
student_code int,
nationality int,
marital_status varchar,
application_choice int,
course varchar,
attendance_mode varchar,
previous_qualification varchar,
previous_qualification_grade decimal,
admission_grade decimal,
displaced varchar,
has_special_needs varchar,
debtor varchar,
paid_tuition_fees varchar,
gender varchar,
scholarship_holder varchar,
date_of_birth int,
age_at_admission int,
international_student varchar,
status varchar
);


-- CREATE OR REPLACE TABLE admissions(
-- student_code int,
-- nationality int,
-- marital_status varchar(255),
-- application_choice int,
-- course varchar(255),
-- attendance_mode varchar(255),
-- previous_qualification varchar(255),
-- previous_qualification_grade decimal,
-- admission_grade decimal,
-- displaced varchar(255),
-- has_special_needs varchar(255),
-- debtor varchar(10),
-- paid_tuition_fees varchar(10),
-- gender varchar(15),
-- scholarship_holder varchar(10),
-- date_of_birth int,
-- age_at_admission int,
-- international_student varchar(10),
-- status varchar(255)
-- );



CREATE OR REPLACE pipe "ADMISSIONS_PIPE"
AUTO_INGEST = TRUE
INTEGRATION = 'EDUCATION_DATA_EVENT'
AS 
COPY INTO admissions
FROM @ADMISSIONS_STAGE
file_format = (type = 'csv');

alter pipe ADMISSIONS_PIPE REFRESH;


SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
             TABLE_NAME=>'admissions', 
             START_TIME=>DATEADD(HOURS,-1, CURRENT_TIMESTAMP())
           ));


ls @ADMISSIONS_STAGE;

SELECT * FROM admissions;

-- countries stage
--------------------------------------------
============================================

CREATE OR REPLACE STAGE countries_stage
url='azure://educationinsights.blob.core.windows.net/education-data-blob/countries.csv'
Credentials = (azure_sas_token='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwlacupiytfx&se=2024-06-19T17:24:30Z&st=2024-05-19T09:24:30Z&spr=https&sig=j%2F%2BxbKZZOWfdZiCysf1BV3p1MWQYLb9w4jRJEjlk1Hg%3D');


create or replace table countries(
country_id int,
country_name varchar(100)
);

CREATE OR REPLACE pipe "COUNTRIES_PIPE"
AUTO_INGEST = TRUE
INTEGRATION = 'EDUCATION_DATA_EVENT'
AS 
COPY INTO countries
FROM @COUNTRIES_STAGE
file_format = (type = 'csv');

alter pipe COUNTRIES_PIPE REFRESH;


SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
             TABLE_NAME=>'countries', 
             START_TIME=>DATEADD(HOURS,-1, CURRENT_TIMESTAMP())
           ));


ls @COUNTRIES_STAGE;

SELECT * FROM countries;

-- parents stage
--------------------------------------------
============================================

CREATE OR REPLACE STAGE parents_stage
url='azure://educationinsights.blob.core.windows.net/education-data-blob/parents.csv'
Credentials = (azure_sas_token='?sv=2022-11-02&ss=bfqt&srt=co&sp=rwlacupiytfx&se=2024-06-19T17:24:30Z&st=2024-05-19T09:24:30Z&spr=https&sig=j%2F%2BxbKZZOWfdZiCysf1BV3p1MWQYLb9w4jRJEjlk1Hg%3D');


create or replace table parents(
student_code int,
relationship varchar,
area varchar,
category varchar
);

-- create or replace table parents(
-- student_code int,
-- relationship varchar(55),
-- area varchar(55),
-- category varchar(100)
-- );

CREATE OR REPLACE pipe "PARENTS_PIPE"
AUTO_INGEST = TRUE
INTEGRATION = 'EDUCATION_DATA_EVENT'
AS 
COPY INTO parents
FROM @PARENTS_STAGE
file_format = (type = 'csv');

alter pipe PARENTS_PIPE REFRESH;


SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
             TABLE_NAME=>'parents', 
             START_TIME=>DATEADD(HOURS,-1, CURRENT_TIMESTAMP())
           ));


ls @PARENTS_STAGE;

SELECT * FROM parents;

==========================================

SELECT * FROM admissions;
SELECT * FROM countries;
SELECT * FROM economy;
SELECT * FROM parents;
