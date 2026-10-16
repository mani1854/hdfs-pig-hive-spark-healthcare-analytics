# CSE412 — Big Data & Large-Scale Computing
## Project Proposal

**Project Title:** Hospital Readmission Analytics Pipeline for Diabetic Patients

**Track Selected:** A — Batch ETL & Warehousing: HDFS → Pig → Hive (+ Supplementary Spark MLlib)

**Team Members:** [Student 1 Name] · [Student 2 Name] · [Student 3 Name]

---

## 1. Problem Statement

Every year, millions of patients in the United States get discharged from hospitals and come back within 30 days. This is called a hospital readmission, and it is a serious problem for two reasons.

First, from a patient care standpoint, an early readmission usually means something went wrong — the patient was not stable enough when they were discharged, or they didn't get the right follow-up care, or their condition turned out to be more complicated than expected.

Second, from a financial and policy standpoint, the Centers for Medicare & Medicaid Services (CMS) runs a program called the Hospital Readmissions Reduction Program (HRRP). Under this program, hospitals with too many early readmissions actually get penalized — their Medicare payments are reduced. This has pushed hospitals to take readmission prediction seriously.

Diabetic patients are a particularly important group to study. Diabetes is the 7th leading cause of death in the US and one of the most common reasons for hospital admission. Managing diabetes well requires careful coordination of multiple medications, diet, and follow-up care. Any breakdown in that chain can result in an early readmission.

**Our question:** What patterns in hospital encounter data predict whether a diabetic patient will be readmitted within 30 days — and can we build a big data pipeline that analyzes this at scale?

---

## 2. Why This Needs Big Data Tools

The dataset we are using covers 130 US hospitals over 10 years (1999–2008). The full dataset has **101,766 hospital encounters and 50 features per row**. A single-tool solution (e.g., just Python/pandas) would:

- Load the entire file into RAM on one machine — not scalable
- Have no distributed storage layer (no fault tolerance, no parallelism)
- Not support SQL-style analytics without additional libraries
- Not support distributed ML without significant custom code

A big data pipeline solves all of these. HDFS gives us distributed storage. Pig runs transformation logic as MapReduce jobs across that storage. Hive gives us a SQL warehouse on top. And if we want ML, Spark reads from the same HDFS/Hive layer and trains models in distributed fashion. Each stage's output feeds directly into the next.

---

## 3. Dataset

**Name:** Diabetes 130-US Hospitals for Years 1999–2008

**Source:** UCI Machine Learning Repository
https://archive.ics.uci.edu/ml/datasets/Diabetes+130-US+hospitals+for+years+1999-2008

**Original paper:** Strack, B. et al. (2014). Impact of HbA1c measurement on hospital readmission rates. *BioMed Research International*.

**What's in the data:**
- 101,766 rows, 50 columns
- Patient demographics (age, gender, race)
- Clinical encounter details (days in hospital, number of lab procedures, number of medications, number of diagnoses)
- 3 diagnosis columns with ICD-9 codes
- 17 diabetes medication columns (metformin, insulin, glipizide, etc.)
- Discharge and admission type information
- **Target:** Was the patient readmitted? (`<30` days, `>30` days, `NO`)

**How we will use it:**
We will download the dataset, apply a balanced 10,000-row sample (equal class distribution), convert the 3-class readmission label into binary (`1` = readmitted in under 30 days, `0` = otherwise), and upload it to HDFS as our starting point.

---

## 4. Chosen Tool Combination

We are following **Track A: HDFS → Pig → Hive**, with Apache Spark MLlib added as an extended analytical stage.

| Tool | Version | Role in Pipeline |
|------|---------|-----------------|
| Apache Hadoop (HDFS) | 3.4.1 | Distributed file system — raw data storage, Pig output storage, Hive backend |
| Apache Pig | 0.17.0 | ETL stage — clean raw CSV, filter bad records, cast types, produce binary label |
| Apache Hive | 4.2.0 | Warehouse layer — external table over Pig output, SQL analytics queries |
| Apache Spark MLlib | 3.5.9 | Supplementary ML stage — train Logistic Regression and Random Forest models |
| Python / PySpark | 3.12 / 3.5.9 | Notebook orchestration, Spark feature engineering, results visualization |

**Data flow:**
```
Raw CSV (diabetes_sample.csv)
    → uploaded to HDFS (/cse412_hospital_readmission/raw/)
    → Pig ETL reads from HDFS, writes cleaned output to HDFS (/pig/cleaned/)
    → Hive external table points to HDFS Pig output — runs analytics queries
    → Spark reads from Hive/HDFS — trains and evaluates ML models
```

Each stage produces real output that the next stage reads. There are no synthetic hand-offs.

---

## 5. Why Each Tool

**HDFS** — Provides the shared distributed storage that makes the whole pipeline possible. All three compute tools (Pig, Hive, Spark) read from and write to the same HDFS namespace. Without HDFS, we would need to manually copy files between tools.

**Pig** — The hospital dataset has data quality issues: some values are marked `?`, some column types are inconsistent, and the readmission label needs to be converted from 3-class to binary. Pig's scripting language (Pig Latin) handles these transformation tasks as MapReduce jobs, making the cleaning logic explicit and auditable — which matters in a healthcare context.

**Hive** — After the data is clean, we want to ask questions like "which age group gets readmitted most?" and "does having more diagnoses predict readmission?". Hive lets us do this with familiar SQL syntax on top of HDFS-stored data. Hospital analytics teams typically work with SQL, so Hive makes the pipeline practically usable beyond this class.

**Spark MLlib (supplementary)** — To go beyond descriptive analytics and test whether readmission is actually predictable, we will train classification models. Spark MLlib integrates with the existing HDFS/Hive layer and runs distributed training.

---

## 6. Expected Outcomes

**From HDFS:** A 10,000-row balanced CSV stored reliably in distributed storage, ready to serve both Pig and Hive.

**From Pig:** A cleaned, type-corrected, 50-column dataset with binary readmission labels stored back to HDFS.

**From Hive:** At least 5 analytical reports answering clinical questions about readmission risk:
- Readmission rate by age group
- Readmission rate by gender
- Impact of prior inpatient visits on readmission risk
- Effect of number of diagnoses (comorbidities)
- Impact of medication changes during a visit

**From Spark MLlib (bonus):** A trained Logistic Regression model evaluated on a held-out patient-level test set. We expect a ROC-AUC in the range of 0.62–0.67, consistent with the original published research on this dataset. If our results fall within this range, it confirms the pipeline is working correctly.

---

## 7. Timeline

| Week | Milestone |
|------|-----------|
| Week 1 | Environment setup (HDFS, Pig, Hive, Spark), dataset download, HDFS ingestion working |
| Week 2 | Pig ETL script working, data cleaned and verified in HDFS |
| Week 3 | Hive table created, initial analytics queries running — **Progress Check-in 1** |
| Week 4 | All Hive analytics done, Spark MLlib model trained — **Progress Check-in 2** |
| Week 5 | Final report written, README completed, demo prepared |
| Week 6 | Final submission and live demo |

---

*Submitted for CSE412 — Big Data & Large-Scale Computing*
*Dataset: UCI Diabetes 130-US Hospitals 1999–2008*
*Tools: Hadoop 3.4.1 · Pig 0.17.0 · Hive 4.2.0 · Spark 3.5.9*
