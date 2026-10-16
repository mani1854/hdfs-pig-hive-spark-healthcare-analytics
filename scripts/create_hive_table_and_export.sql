
SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=true;

CREATE DATABASE IF NOT EXISTS cse412_readmission;

DROP TABLE IF EXISTS cse412_readmission.cleaned_encounters;

CREATE EXTERNAL TABLE cse412_readmission.cleaned_encounters (
    patient_nbr STRING,
    race STRING,
    gender STRING,
    age STRING,
    weight STRING,
    admission_type_id STRING,
    discharge_disposition_id STRING,
    admission_source_id STRING,
    time_in_hospital INT,
    payer_code STRING,
    medical_specialty STRING,
    num_lab_procedures INT,
    num_procedures INT,
    num_medications INT,
    number_outpatient INT,
    number_emergency INT,
    number_inpatient INT,
    diag_1 STRING,
    diag_2 STRING,
    diag_3 STRING,
    number_diagnoses INT,
    max_glu_serum STRING,
    A1Cresult STRING,
    metformin STRING,
    repaglinide STRING,
    nateglinide STRING,
    chlorpropamide STRING,
    glimepiride STRING,
    acetohexamide STRING,
    glipizide STRING,
    glyburide STRING,
    tolbutamide STRING,
    pioglitazone STRING,
    rosiglitazone STRING,
    acarbose STRING,
    miglitol STRING,
    troglitazone STRING,
    tolazamide STRING,
    examide STRING,
    citoglipton STRING,
    insulin STRING,
    glyburide_metformin STRING,
    glipizide_metformin STRING,
    glimepiride_pioglitazone STRING,
    metformin_rosiglitazone STRING,
    metformin_pioglitazone STRING,
    change STRING,
    diabetesMed STRING,
    label INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/cse412_hospital_readmission/pig/cleaned';

-- Verify table record count
SELECT COUNT(*) AS total_cleaned_records FROM cse412_readmission.cleaned_encounters;

-- Export prepared features to HDFS for Spark MLlib
INSERT OVERWRITE DIRECTORY '/cse412_hospital_readmission/hive/model_input'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM cse412_readmission.cleaned_encounters;
