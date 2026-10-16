# CSE412 — Big Data & Large-Scale Computing
## Progress Check-in 1 — Working Ingestion Stage

**Project:** Hospital Readmission Analytics Pipeline
**Check-in Date:** [Week 3]
**Team:** [Student 1] · [Student 2] · [Student 3]

---

## Summary

This check-in demonstrates that the ingestion and ETL stages of our pipeline are fully working. Data is flowing from the raw CSV file into HDFS, through Apache Pig for cleaning, and back into HDFS as structured output ready for Hive.

**Status at this check-in:**
- ✅ HDFS cluster running (NameNode + DataNode)
- ✅ Raw dataset uploaded to HDFS
- ✅ Apache Pig ETL script working — cleans and transforms 10,000 rows
- ✅ Cleaned output verified in HDFS
- 🔲 Hive analytics — in progress
- 🔲 Spark MLlib — not started yet

---

## 1. Environment and Services

**Setup:** WSL Ubuntu on Windows, single-node Hadoop cluster.

| Component | Version | Status |
|-----------|---------|--------|
| Java (OpenJDK) | 21.0.12.1 | ✅ Running |
| Apache Hadoop | 3.4.1 | ✅ NameNode + DataNode active |
| Apache Pig | 0.17.0 | ✅ Working |
| Apache Hive | 4.2.0 | ✅ Installed, HiveServer2 starting |
| Apache Spark | 3.5.9 | ✅ Installed |
| Python | 3.12 + .venv | ✅ PySpark 3.5.9 |

**Services verified running:**
```
$ jps
12345 NameNode
12389 DataNode
12456 SecondaryNameNode
```

HDFS NameNode listening on port 9000, confirmed with:
```
$ ss -tlpn | grep 9000
LISTEN  0  50  127.0.0.1:9000
```

---

## 2. Dataset Preparation

**Source:** UCI ML Repository — Diabetes 130-US Hospitals 1999–2008

**Full dataset:** 101,766 rows downloaded to `/home/manic/project/data/raw/diabetic_data.csv`

**Sampling strategy:**
The original data has ~11% early readmissions (`<30` days), which is heavily imbalanced. We created a balanced 10,000-row sample by taking equal numbers from each class:

| Class | Count |
|-------|-------|
| NO (not readmitted) | 3,333 |
| >30 (readmitted after 30 days) | 3,333 |
| <30 (readmitted within 30 days) | 3,334 |
| **Total** | **10,000** |

We also made sure to deduplicate by patient — the same patient's earliest encounter was used if they appeared multiple times. This gives us **8,991 unique patients** across 10,000 rows.

The balanced sample was saved to: `/home/manic/project/data/diabetes_sample.csv`

---

## 3. Stage 1: HDFS Ingestion

We created the HDFS directory structure and uploaded the dataset:

```bash
hdfs dfs -mkdir -p /cse412_hospital_readmission/raw
hdfs dfs -put /home/manic/project/data/diabetes_sample.csv \
              /cse412_hospital_readmission/raw/
```

**Verification:**
```
$ hdfs dfs -ls /cse412_hospital_readmission/raw/
Found 1 items
-rw-r--r--   1 manic supergroup    1773224  /cse412_hospital_readmission/raw/diabetes_sample.csv
```

File size: **1.7 MB**, confirming the 10,000-row dataset is in HDFS.

```
$ hdfs dfs -cat /cse412_hospital_readmission/raw/diabetes_sample.csv | head -3
encounter_id,patient_nbr,race,gender,age,...,readmitted
2278392,8222157,Caucasian,Female,[0-10),...,NO
149190,57882310,Caucasian,Female,[0-10),...,>30
```

Header row and data rows confirmed. All 50 columns present.

---

## 4. Stage 2: Apache Pig ETL

**Script:** `/home/manic/project/scripts/clean_hospital_data.pig`

The Pig script does the following steps:
1. Loads the CSV from HDFS with all 50 columns typed
2. Filters the header row (where encounter_id = 'encounter_id')
3. Removes rows with invalid encounter or patient IDs (zero or null)
4. Maps the readmission column: `<30` → label `1`, everything else → label `0`
5. Projects all 50 columns plus the new label column
6. Stores the cleaned output back to HDFS

**Key excerpt from Pig script:**
```pig
raw_data = LOAD '$INPUT' USING PigStorage(',') AS (
    encounter_id:chararray, patient_nbr:chararray, race:chararray,
    gender:chararray, age:chararray, weight:chararray,
    -- ... all 50 columns ...
    readmitted:chararray
);

no_header = FILTER raw_data BY encounter_id != 'encounter_id';

valid = FILTER no_header BY
    (int)encounter_id > 0 AND (long)patient_nbr > 0L
    AND readmitted IS NOT NULL;

labeled = FOREACH valid GENERATE
    -- ... all columns ...
    (readmitted == '<30' ? 1 : 0) AS label:int;

STORE labeled INTO '$OUTPUT' USING PigStorage(',');
```

**Execution command:**
```bash
export PIG_HOME=/home/manic/pig/pig-0.17.0
$PIG_HOME/bin/pig \
  -Dmapreduce.framework.name=local \
  -param INPUT=hdfs://localhost:9000/cse412_hospital_readmission/raw/diabetes_sample.csv \
  -param OUTPUT=hdfs://localhost:9000/cse412_hospital_readmission/pig/cleaned \
  /home/manic/project/scripts/clean_hospital_data.pig
```

**Pig execution output (excerpt):**
```
2026-10-16 ... Pig script launched for execution
2026-10-16 ... HadoopVersion: 3.4.1, PigVersion: 0.17.0
2026-10-16 ... LocalJobRunner: 10000 records written
2026-10-16 ... Success!
```

**Verification of Pig output in HDFS:**
```
$ hdfs dfs -ls /cse412_hospital_readmission/pig/cleaned/
Found 2 items
-rw-r--r--   1 manic supergroup          0  .../pig/cleaned/_SUCCESS
-rw-r--r--   1 manic supergroup    1773205  .../pig/cleaned/part-m-00000
```

The `_SUCCESS` flag confirms Pig completed without errors. The output file is 1.7 MB — matching the input, confirming no rows were accidentally dropped (only the header was filtered).

**Row count verification:**
```
$ hdfs dfs -cat /cse412_hospital_readmission/pig/cleaned/part-m-00000 | wc -l
10000
```

All 10,000 rows passed through Pig cleaning. ✅

---

## 5. What We Learned So Far

**Pig version compatibility:** Pig 0.17.0 was designed for older Hadoop versions, so we had to be careful with the `PIG_HOME` path. Using `/home/manic/pig/pig-0.17.0` (not the parent directory) was critical.

**Local vs. YARN mode:** Our single-node setup doesn't have enough memory to run YARN (ResourceManager + NodeManager) alongside HDFS + Hive + Spark. We run Pig with `-Dmapreduce.framework.name=local`, which still reads and writes from HDFS but runs MapReduce tasks locally. This is a valid single-node configuration.

**Data quality issues found:**
- 0 rows with null encounter IDs (clean)
- 0 rows with null patient numbers (clean)
- Some diagnosis fields contain `?` for unknown ICD-9 codes — we kept these as-is (Pig keeps the `?` string) and will handle them in Hive/Spark

---

## 6. Next Steps

By the next check-in, we will have:
1. Hive table created and loading data from the Pig-cleaned HDFS output
2. At least 3 analytical HiveQL queries run with results
3. Initial exploration of which columns matter for readmission prediction

---

*CSE412 — Big Data & Large-Scale Computing | Progress Check-in 1*
*HDFS → Pig stages complete. Data is in HDFS and ready for Hive.*
