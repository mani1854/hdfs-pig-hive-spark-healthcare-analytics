
raw = LOAD '$INPUT' USING PigStorage(',') AS (
    f0:chararray, f1:chararray, f2:chararray, f3:chararray, f4:chararray,
    f5:chararray, f6:chararray, f7:chararray, f8:chararray, f9:chararray,
    f10:chararray, f11:chararray, f12:chararray, f13:chararray, f14:chararray,
    f15:chararray, f16:chararray, f17:chararray, f18:chararray, f19:chararray,
    f20:chararray, f21:chararray, f22:chararray, f23:chararray, f24:chararray,
    f25:chararray, f26:chararray, f27:chararray, f28:chararray, f29:chararray,
    f30:chararray, f31:chararray, f32:chararray, f33:chararray, f34:chararray,
    f35:chararray, f36:chararray, f37:chararray, f38:chararray, f39:chararray,
    f40:chararray, f41:chararray, f42:chararray, f43:chararray, f44:chararray,
    f45:chararray, f46:chararray, f47:chararray, f48:chararray, f49:chararray
);

-- Remove CSV header
without_header = FILTER raw BY f0 != 'encounter_id';

-- Filter for known target readmission values
known_target = FILTER without_header BY (f49 == '<30' OR f49 == '>30' OR f49 == 'NO');

-- Project ALL columns from dataset (50 attributes)
projected = FOREACH known_target GENERATE
    f1 AS patient_nbr,
    (f2 == '?' ? 'Unknown' : f2) AS race,
    f3 AS gender,
    f4 AS age,
    (f5 == '?' ? 'Unknown' : f5) AS weight,
    f6 AS admission_type_id,
    f7 AS discharge_disposition_id,
    f8 AS admission_source_id,
    (int)f9 AS time_in_hospital,
    (f10 == '?' ? 'Unknown' : f10) AS payer_code,
    (f11 == '?' ? 'Unknown' : f11) AS medical_specialty,
    (int)f12 AS num_lab_procedures,
    (int)f13 AS num_procedures,
    (int)f14 AS num_medications,
    (int)f15 AS number_outpatient,
    (int)f16 AS number_emergency,
    (int)f17 AS number_inpatient,
    (f18 == '?' ? 'Unknown' : f18) AS diag_1,
    (f19 == '?' ? 'Unknown' : f19) AS diag_2,
    (f20 == '?' ? 'Unknown' : f20) AS diag_3,
    (int)f21 AS number_diagnoses,
    (f22 == '?' ? 'None' : f22) AS max_glu_serum,
    (f23 == '?' ? 'None' : f23) AS A1Cresult,
    f24 AS metformin,
    f25 AS repaglinide,
    f26 AS nateglinide,
    f27 AS chlorpropamide,
    f28 AS glimepiride,
    f29 AS acetohexamide,
    f30 AS glipizide,
    f31 AS glyburide,
    f32 AS tolbutamide,
    f33 AS pioglitazone,
    f34 AS rosiglitazone,
    f35 AS acarbose,
    f36 AS miglitol,
    f37 AS troglitazone,
    f38 AS tolazamide,
    f39 AS examide,
    f40 AS citoglipton,
    f41 AS insulin,
    f42 AS glyburide_metformin,
    f43 AS glipizide_metformin,
    f44 AS glimepiride_pioglitazone,
    f45 AS metformin_rosiglitazone,
    f46 AS metformin_pioglitazone,
    f47 AS change,
    f48 AS diabetesMed,
    (f49 == '<30' ? 1 : 0) AS label;

-- Filter corrupted records
cleaned = FILTER projected BY
    patient_nbr IS NOT NULL AND patient_nbr != '' AND
    time_in_hospital IS NOT NULL AND
    num_lab_procedures IS NOT NULL AND
    num_procedures IS NOT NULL AND
    num_medications IS NOT NULL AND
    number_outpatient IS NOT NULL AND
    number_emergency IS NOT NULL AND
    number_inpatient IS NOT NULL AND
    number_diagnoses IS NOT NULL;

STORE cleaned INTO '$OUTPUT' USING PigStorage('\t');
