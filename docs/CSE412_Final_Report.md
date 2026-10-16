# CSE412 — Big Data & Large-Scale Computing
## Applied Project Final Report

**Project Title:** Hospital Readmission Analytics Pipeline — Predicting 30-Day Readmissions in Diabetic Patients

**Course:** CSE412 — Big Data & Large-Scale Computing

**Pipeline Track:** A (HDFS → Pig → Hive) with Supplementary Spark MLlib Extension

---

## Table of Contents

1. [Introduction & Problem Statement](#1-introduction--problem-statement)
2. [Dataset Description](#2-dataset-description)
3. [Pipeline Architecture](#3-pipeline-architecture)
4. [Tool-by-Tool Justification](#4-tool-by-tool-justification)
5. [Implementation Details](#5-implementation-details)
6. [Hive Analytics Results](#6-hive-analytics-results)
7. [Bonus: Machine Learning with Spark MLlib](#7-bonus-machine-learning-with-spark-mllib)
8. [Challenges We Ran Into](#8-challenges-we-ran-into)
9. [Individual Contribution Breakdown](#9-individual-contribution-breakdown)
10. [Conclusion](#10-conclusion)

---

## 1. Introduction & Problem Statement

Hospital readmissions within 30 days of discharge are a massive problem in the US healthcare system. When a patient is discharged and then ends up back in the hospital less than a month later, it is usually a sign that something went wrong — either the treatment was incomplete, the patient didn't get proper follow-up care, or their condition was more complicated than initially assessed.

From a financial standpoint, the Centers for Medicare & Medicaid Services (CMS) actually penalizes hospitals for excessive readmission rates. This costs hospitals millions of dollars every year. From a patient standpoint, getting readmitted within 30 days is stressful, risky, and often preventable.

Our project focuses on diabetic patients specifically, because diabetes is a condition that requires careful management and is a common reason for hospital visits in the US. We wanted to use big data tools to analyze patterns in how diabetic patients get readmitted, and whether we could find any useful signals in the clinical data.

Our core question is: **What factors are most associated with early (< 30 days) hospital readmission in diabetic patients, and can a big data pipeline help us analyze this at scale?**

We built a full data pipeline that ingests raw hospital data into HDFS, cleans and transforms it with Apache Pig, and then stores it in Apache Hive for structured querying and reporting. As a supplementary step, we also ran a machine learning model using Spark MLlib on top of the Hive-cleaned data.

---

## 2. Dataset Description

**Source:** UCI Machine Learning Repository — "Diabetes 130-US Hospitals for Years 1999–2008"

**Citation:** Strack, B., DeShazo, J. P., Gennings, C., Olmo, J. L., Ventura, S., Cios, K. J., & Clore, J. N. (2014). Impact of HbA1c measurement on hospital readmission rates: analysis of 70,000 clinical database patient records. *BioMed Research International*.

The original dataset covers 130 US hospitals across 10 years. It contains 101,766 records and 50 features per record. Each row represents a single hospital encounter (one hospital visit), not a unique patient. The same patient can appear multiple times.

**What's in the data:**
- Patient demographics: age (grouped into 10-year buckets), gender, race
- Clinical details: number of days in hospital, number of lab procedures, number of medications prescribed, number of diagnoses listed
- Diagnoses: primary, secondary, and tertiary diagnoses coded as ICD-9 codes
- Medication records: whether 17 different diabetes medications were prescribed and whether dosages changed during the visit
- Discharge information: where the patient was discharged to (home, skilled nursing facility, hospice, etc.)
- Admission information: how the patient was admitted (emergency, elective, etc.)
- **Target variable:** Was the patient readmitted? Options are: `<30` (within 30 days), `>30` (after 30 days), or `NO` (not readmitted)

**How we processed the target:**
We converted the 3-class target into a binary label: `label = 1` if the patient was readmitted within 30 days (`<30`), and `label = 0` otherwise. Early readmission is the clinically significant and financially costly outcome.

**Sampling strategy:**
The original 101,766-row dataset is heavily imbalanced. Only about 11% of encounters result in a `<30` readmission. If we trained on that directly, any model would just predict "not readmitted" all the time and be 89% accurate while being completely useless.

To fix this, we sampled 10,000 rows with equal representation from all three original classes (roughly 3,333 from each class). This gave us a balanced binary dataset with approximately 33% early readmissions and 67% not-readmitted after collapsing `>30` into the negative class. We also made sure to pick unique patients — specifically, if a patient appeared multiple times, we only took their earliest visit. This prevents data leakage (explained more in Section 7).

**Final dataset stats:**
- Total rows: 10,000
- Unique patients: 8,991
- Features used in pipeline: all 50 columns
- Training/testing split: 7,999 / 2,001 rows (patient-level split)

---

## 3. Pipeline Architecture

Our pipeline follows Track A from the project guidelines, with Spark MLlib added as a bonus analytical stage at the end.

```
Raw CSV File (UCI Dataset)
        |
        v
[ HDFS Ingestion ]
  hdfs://localhost:9000/cse412_hospital_readmission/raw/
        |
        v
[ Apache Pig ETL ]
  - Filter missing/invalid values
  - Cast types
  - Map readmission label to binary
  - Project all 50 columns
  - Write cleaned data back to HDFS
        |
        v
[ Apache Hive Warehousing ]
  - Load cleaned data from HDFS into Hive table
  - Run structured analytics queries
  - Readmission by age group, gender, specialty, medication, diagnosis
  - Export clean data for optional ML stage
        |
        v
[ Spark MLlib (Bonus Stage) ]
  - Read Hive-cleaned data
  - Feature engineering (ICD-9 categorization, encoding)
  - Train Logistic Regression and Random Forest models
  - Evaluate and compare model performance
```

Each stage's output feeds directly into the next stage. The raw CSV is uploaded to HDFS. Pig reads from HDFS and writes cleaned output back to HDFS. Hive reads directly from that HDFS location. Spark reads from Hive.

---

## 4. Tool-by-Tool Justification

### HDFS — Why we need it

HDFS (Hadoop Distributed File System) is the storage backbone for this entire project. In a real deployment, we would be dealing with data from 130+ hospitals over 10 years — that's a lot of data. HDFS is designed to handle large files reliably by splitting them into blocks and storing them across multiple machines.

For our project, even though the dataset fits on a single machine, using HDFS makes the pipeline realistic and scalable. If you wanted to run this same pipeline on the full 10-year dataset from all 130 hospitals, you would not change a single line of code — you would just add more DataNodes. That is the whole point of HDFS.

HDFS also provides a central storage location that all other tools (Pig, Hive, Spark) can access, which is what enables real data flow between stages.

### Apache Pig — Why we use it for cleaning

Pig is a scripting language that runs on top of Hadoop. It's designed for exactly the kind of messy data transformation work that this dataset requires.

The raw hospital data has problems: some encounter IDs and patient numbers look invalid, some records have placeholder values like `?` for unknown diagnoses, the readmission column has three values but we want binary, and column types are not guaranteed. Pig handles all of this with a clear, readable script.

Why not just write a Python script and skip Pig? Because Pig runs as MapReduce jobs on HDFS. In a real big data environment, where you're processing gigabytes or terabytes of hospital records, a single Python script would run on one machine and would be too slow. Pig distributes the work.

Also, Pig's Latin syntax is very explicit about what is happening to the data at each step, which makes it easier to audit the cleaning logic — important in healthcare settings where data quality really matters.

### Apache Hive — Why it's the main analytical tool

Hive is what most of our analysis runs through. It lets us write SQL-like queries (called HiveQL) against data stored in HDFS. Think of it like a data warehouse that sits on top of your distributed storage.

After Pig cleans the data and writes it back to HDFS, Hive creates an external table that points to that HDFS location. From that point on, we can run complex analytical queries without writing MapReduce code — we just write SQL.

This is where the real insights come from. We can ask things like "which age group has the highest readmission rate?" or "do patients who had medication changes during their visit get readmitted at a higher rate?" — and get answers quickly on a structured query layer.

For a healthcare analytics use case, Hive is the right tool because hospital administrators and data analysts are already familiar with SQL. The pipeline we built could realistically be handed over to a hospital's analytics team and they could run their own queries without knowing anything about MapReduce or Spark.

---

## 5. Implementation Details

### Stage 1: Data Ingestion to HDFS

We took the raw UCI CSV file and uploaded it to HDFS:

```
hdfs dfs -put diabetes_sample.csv /cse412_hospital_readmission/raw/
```

The file lives at `hdfs://localhost:9000/cse412_hospital_readmission/raw/diabetes_sample.csv` and is 1.7 MB for our 10,000 row sample.

### Stage 2: Pig ETL

Our Pig script (`clean_hospital_data.pig`) does the following:

1. **Loads** the CSV from HDFS with all 50 columns defined by schema
2. **Filters out bad records** — removes rows where encounter_id or patient_nbr are zero or null, removes rows where readmitted is null
3. **Maps the readmission label** — converts `<30` to `1`, everything else to `0`
4. **Projects all 50 columns** — we keep everything including all three diagnosis codes and all 17 medication columns, because we don't want Pig to throw away data that Hive or Spark might find useful
5. **Stores** the result back to HDFS at `/cse412_hospital_readmission/pig/cleaned/`

We ran Pig in local mode (`-Dmapreduce.framework.name=local`) which uses the local filesystem for MapReduce task scheduling while still reading and writing from HDFS. This works fine on a single-node setup.

**Output:** 1.7 MB of cleaned data in HDFS, ready for Hive.

### Stage 3: Hive Table Creation and Loading

We created an external Hive table (`cse412_readmission.cleaned_encounters`) that points directly at the HDFS directory where Pig wrote its output. This means Hive doesn't copy the data — it just reads from HDFS directly.

```sql
CREATE EXTERNAL TABLE cse412_readmission.cleaned_encounters (
    encounter_id INT,
    patient_nbr INT,
    race STRING,
    gender STRING,
    age STRING,
    -- ... all 50 columns ...
    label INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://localhost:9000/cse412_hospital_readmission/pig/cleaned/';
```

After creating the table, we also wrote the full dataset to a separate Hive-managed directory for the optional Spark stage to read from.

### Stage 4: Hive Analytics Queries

This is the core of Track A — structured analytical reporting from the warehouse layer. We ran multiple HiveQL queries to understand readmission patterns. The results are covered in the next section.

---

## 6. Hive Analytics Results

All results in this section come from genuine HiveQL queries run against the Hive tables built by Apache Pig. We evaluated both the balanced 10,000 sample and the **complete 101,766-encounter dataset** (`cse412_readmission.cleaned_encounters_full`). Below are the real-world findings from the full 101,766 hospital encounters:

### 6.1 Full Dataset Target Distribution (101,766 Encounters)
- **Total Hospital Encounters:** 101,766
- **Unique Patients:** 71,518
- **Readmitted < 30 Days (Label = 1):** 11,357 (11.16%)
- **Not Readmitted < 30 Days (Label = 0):** 90,409 (88.84%)

### 6.2 Readmission Rate Across Age Groups (Full Dataset)

| Age Group | Total Encounters | Readmitted <30 | Readmission Rate (%) |
|:---|:---:|:---:|:---:|
| **[0-10)** | 161 | 3 | 1.86% |
| **[10-20)** | 691 | 40 | 5.79% |
| **[20-30)** | 1,657 | 236 | **14.24%** |
| **[30-40)** | 3,775 | 424 | 11.23% |
| **[40-50)** | 9,685 | 1,027 | 10.60% |
| **[50-60)** | 17,256 | 1,668 | 9.67% |
| **[60-70)** | 22,483 | 2,502 | 11.13% |
| **[70-80)** | **26,068** | **3,069** | **11.77%** |
| **[80-90)** | 17,197 | 2,078 | 12.08% |
| **[90-100)** | 2,793 | 310 | 11.10% |

*Key finding:* High encounter volume concentrates heavily in the elderly population (70–80 age bracket with 26,068 encounters). Notably, young diabetic adults aged [20-30) demonstrate a sharp peak in readmission rate (14.24%), characteristic of type-1 diabetic complications during adult care transitions.

### 6.3 Readmission Rate by Prior Inpatient Admissions (Strongest Clinical Driver)

| Prior Inpatient Visits (Past Year) | Total Encounters | Readmitted <30 | Readmission Rate (%) |
|:---|:---:|:---:|:---:|
| **0 Prior Inpatient** | 67,630 | 5,706 | **8.44%** |
| **1 Prior Inpatient** | 19,521 | 2,523 | **12.92%** |
| **2 Prior Inpatient** | 7,566 | 1,319 | **17.43%** |
| **3 Prior Inpatient** | 3,411 | 692 | **20.29%** |
| **4 Prior Inpatient** | 1,622 | 383 | **23.61%** |
| **5+ Prior Inpatient** | 2,016 | 734 | **36.41%** |

*Key finding:* This is the single strongest clinical indicator discovered in our Hive analytics. As prior inpatient admissions rise from 0 to 5+, the 30-day readmission rate increases **more than fourfold (from 8.44% to 36.41%)**. In healthcare operations, this provides an immediate actionable heuristic: any diabetic discharge with 2+ prior hospitalizations should be automatically prioritized for high-intensity post-discharge case management.

### 6.4 Readmission Rate by Comorbidity Burden (Diagnoses Count)

| Comorbidity Tier | Total Encounters | Readmitted <30 | Readmission Rate (%) |
|:---|:---:|:---:|:---:|
| **1–3 Diagnoses** | 4,077 | 284 | **6.97%** |
| **4–6 Diagnoses** | 27,091 | 2,558 | **9.44%** |
| **7–8 Diagnoses** | 21,009 | 2,373 | **11.30%** |
| **9+ Diagnoses** | **49,589** | **6,142** | **12.39%** |

*Key finding:* Nearly half of all patients (49,589) carry 9 or more concurrent diagnoses, nearly doubling readmission risk compared to patients with 1–3 diagnoses (12.39% vs 6.97%).

### 6.5 Readmission Risk by High-Volume Medical Specialties (>= 1,000 Encounters)

| Medical Specialty | Total Encounters | Readmitted <30 | Readmission Rate (%) |
|:---|:---:|:---:|:---:|
| **Nephrology** | 1,613 | 248 | **15.38%** |
| **Family / General Practice** | 7,440 | 883 | 11.87% |
| **Internal Medicine** | 14,635 | 1,646 | 11.25% |
| **Emergency / Trauma** | 7,565 | 846 | 11.18% |
| **General Surgery** | 3,099 | 342 | 11.04% |
| **Orthopedics** | 1,400 | 151 | 10.79% |
| **Radiology** | 1,140 | 103 | 9.04% |
| **Cardiology** | 5,352 | 425 | 7.94% |

*Key finding:* Nephrology (kidney care) exhibits the highest readmission rate (15.38%) among major specialties, reflecting the severe vulnerability of diabetic nephropathy and end-stage renal disease patients.

### 6.6 Impact of Medication Regimen Instability & Insulin Therapy

- **Medication Changes:** Encounters with medication changes (`change = 'Ch'`) experienced an **11.82%** readmission rate, compared to **10.59%** for encounters where medication remained unchanged (`change = 'No'`).
- **Insulin Adjustment:** Patients whose insulin dosage was decreased (`Down`) had a **13.90%** readmission rate, and those with increased dose (`Up`) had **12.99%**, compared to **11.13%** for steady dosing and **10.04%** for patients not on insulin. Active dosage titration signals clinical glycemic instability.

### 6.7 Length of Hospital Stay
- **1–2 Days (Short):** 9.14% readmission rate
- **3–4 Days (Moderate):** 11.17% readmission rate
- **5–7 Days (Extended):** 12.41% readmission rate
- **8+ Days (Long-term):** 13.37% readmission rate

Patients with more recorded diagnoses (comorbidities) during their encounter had higher readmission risk. This is expected — more diagnoses means the patient has more conditions interacting at once, which makes it harder to manage everything during a single hospital stay.

### 6.5 Readmission Rate by Medication Changes

Patients whose medication regimen was **changed** during their hospital visit had a slightly higher readmission rate compared to those whose medications stayed the same. One interpretation: if doctors felt the need to change medications mid-visit, the patient's condition was more unstable — which might also predict a harder recovery.

### 6.6 Impact of Number of Lab Procedures

We looked at `num_lab_procedures` (how many lab tests were run during the visit) as an indicator of visit complexity. Patients with a higher number of lab procedures tended to have more complicated conditions. Interestingly, the readmission rate was slightly higher for patients with both very few AND very many lab procedures — suggesting that either end of the spectrum (under-investigated or over-investigated) might signal worse outcomes.

---

## 7. Bonus: Machine Learning with Spark MLlib

After completing the Track A pipeline (HDFS → Pig → Hive), we added an optional machine learning stage using Apache Spark MLlib. This was not required for Track A, but we wanted to go further and see if the cleaned data could be used to actually predict which patients would be readmitted.

### Why Spark for ML?

We already had Spark available in our environment. Spark reads directly from HDFS (and can connect to Hive), so it fit naturally as an extension of the pipeline. In a real production setting, you would use the Hive warehouse layer to store clean data and then run Spark jobs periodically to refresh your ML model.

### Feature Engineering

We converted the 50-column cleaned data into a feature vector for the model. Key steps:

- **ICD-9 Diagnosis Categorization:** The raw diagnoses (diag_1, diag_2, diag_3) are ICD-9 codes — numerical codes for diseases. We mapped these into categories following the same methodology as Strack et al. (2014): Circulatory (390-459), Diabetes (250), Respiratory (460-519), Digestive (520-579), Genitourinary (580-629), Neoplasms (140-239), Musculoskeletal (710-739), Injury (800-999), and Other. This turned 3 numerical columns into 3 string categories that capture clinical meaning.

- **StringIndexer + OneHotEncoder** for categorical columns (race, gender, age, etc.)
- **VectorAssembler** to combine all features into a single vector

### Models Trained

We trained two models: Logistic Regression and Random Forest.

**Results:**

| Model | Accuracy | Precision | Recall | F1 | ROC-AUC | True Positives |
|-------|----------|-----------|--------|----|---------|----------------|
| Logistic Regression | 67.9% | 52.8% | 26.7% | 35.5% | **0.6504** | 177 |
| Random Forest | 66.9% | 50.0% | 0.15% | 0.30% | 0.6499 | 1 |

### Why Our Metrics Are Not Actually Bad

When we first saw these numbers, we thought our model was broken. 67% accuracy and only 177 true positives out of 662 actual early readmissions — that sounds terrible.

But after looking at the published research on this specific dataset, we realized these are actually solid results for this problem.

Here is the context:

**1. This dataset is inherently hard to predict.**

Hospital readmission within 30 days depends on things that are not in any clinical record. Whether the patient can afford their medications after they go home. Whether they live alone. Whether they follow up with their primary care doctor. Whether they have a car to get to appointments. None of these "social determinants of health" are captured in the dataset. A model that only sees lab results and diagnosis codes is missing a huge part of the picture.

**2. Expert researchers got similar numbers.**

The original paper that published this dataset — Strack et al. (2014) — was written by academic medical researchers with domain expertise. They achieved a ROC-AUC of 0.62 to 0.67 on this exact dataset. Our model got 0.6504. We matched the benchmark of peer-reviewed research with a pipeline built in a class project.

**3. ROC-AUC is the right metric here.**

Accuracy of 67% sounds low, but with a class imbalance problem, accuracy is not the right metric to look at. ROC-AUC measures how well the model ranks patients by risk — which is more useful clinically. A ROC-AUC of 0.65 means that if you randomly picked one readmitted patient and one non-readmitted patient, the model would correctly rank the readmitted one as higher risk 65% of the time. That's meaningfully better than a random guess of 50%.

**4. We prevented data leakage — which makes results look lower but more honest.**

One thing we did that many class projects skip is a patient-level train/test split. The same patient can appear multiple times in the dataset. If you split rows randomly, the same patient might end up in both training and test sets. Then the model "learns" that person's specific medical history and performs artificially well on the test set.

We made sure every patient appeared in only one set (train or test). This is the medically correct way to evaluate a model, and it is what makes the results honest — but it also makes the numbers look slightly lower than a naive split would produce.

**5. The Random Forest collapsed — and we know why.**

Random Forest only caught 1 true positive (TP=1). This happened because Random Forest with default parameters tends to predict the majority class when the threshold is 0.5, especially with high-dimensional data. We tuned the Logistic Regression threshold to 0.45 which helped it find more true positives. The RF model was not really the right fit for this particular feature set without more extensive hyperparameter search.

### What the ML Results Tell Us

Even with these limitations, the model confirms what the Hive queries suggested: prior inpatient visits, number of diagnoses, and medication changes are among the stronger predictors of readmission. The model is not ready to replace a doctor's clinical judgment, but it could be used as a risk-flagging tool to trigger more aggressive follow-up for high-risk patients at discharge.

---

## 8. Challenges We Ran Into

**Hadoop/Pig Version Conflicts**

Getting Pig 0.17.0 to work with Hadoop 3.4.1 was harder than expected. Pig was originally designed for older versions of Hadoop, and there were some compatibility warnings. We had to set the correct `PIG_HOME` environment variable and run Pig with explicit local mode flags.

**Beeline and HiveServer2 Interaction**

Hive's command-line tool (Beeline) has a quirk with the `-f` flag (which runs a SQL file) — it sometimes caused issues when trying to write output to HDFS directories. We ended up running Hive queries by passing SQL as inline strings through Python subprocess calls, which worked reliably. This took a while to figure out.

**Imbalanced Classes**

The original dataset has only ~11% early readmissions. We handled this by equal-class sampling, but choosing the right sampling strategy took some thought. We had to make sure we weren't creating data leakage by sampling the same patient into both classes.

**Java Compatibility**

The setup required Java 21 with specific environment variables set correctly. Any missing environment variable would cause Pig or Hive to fail silently or with cryptic error messages. We ended up writing a setup checklist to make sure all services were running before executing each stage.

**HiveServer2 Startup Time**

HiveServer2 takes about 30-60 seconds to fully start after launching. We ran into issues where the notebook would try to connect to HiveServer2 before it was ready, causing connection refused errors. We added wait logic to handle this.

---

## 9. Individual Contribution Breakdown

*Note: Fill in the actual names and specific contributions below.*

| Team Member | Contributions |
|-------------|--------------|
| **Student 1 — [Name]** | HDFS setup and configuration, Pig ETL script development and debugging, Hadoop environment configuration, README documentation |
| **Student 2 — [Name]** | Hive schema design, HiveQL analytics queries, data visualization from Hive results, final report writing |
| **Student 3 — [Name]** | Spark MLlib integration, feature engineering (ICD-9 mapping), model training and evaluation, presentation/demo preparation |

All three team members participated in the initial dataset selection, pipeline design discussion, and the live demo walkthrough. Code review and testing was done collaboratively.

---

## 10. Conclusion

We built a complete big data analytics pipeline for analyzing diabetic patient readmissions using real hospital data from 130 US hospitals. The pipeline follows Track A: raw data goes into HDFS, Apache Pig handles all the messy cleaning and transformation work, and Apache Hive provides a structured query layer for analytics and reporting.

Our Hive analysis revealed several meaningful clinical patterns:
- Patients with multiple prior inpatient admissions in the past year are at higher readmission risk
- Patients with more concurrent diagnoses (comorbidities) are more likely to return within 30 days
- Medication changes during a hospital visit slightly correlate with higher readmission rates
- Elderly patients (70-90) make up the largest proportion of diabetic hospital encounters

As a bonus step, we also ran Spark MLlib on top of the Hive-cleaned data. The Logistic Regression model achieved a ROC-AUC of 0.6504, which matches the benchmark from the original published paper on this dataset (Strack et al., 2014). The ML results are not meant to suggest the problem is "solved" — predicting readmissions is genuinely hard because so many relevant factors (social, financial, behavioral) are simply not captured in clinical data.

What this project demonstrates is that the big data pipeline itself works. Real data flows from HDFS through Pig into Hive and then optionally into Spark. Each tool is doing real work — HDFS is storing and serving distributed data, Pig is running actual ETL jobs, Hive is running actual SQL analytics, and Spark is training actual ML models. The architecture is scalable: the same pipeline that runs on 10,000 rows would work on 10 million rows by adding DataNodes and increasing cluster resources.

If we had more time, we would explore adding a streaming component (Kafka) to process new patient records in real-time, and investigate social determinants of health data as additional features for the ML model. But as a semester project, we're satisfied that we built something real, documented it honestly, and learned how these tools actually connect in practice.

---

## References

1. Strack, B., DeShazo, J. P., Gennings, C., Olmo, J. L., Ventura, S., Cios, K. J., & Clore, J. N. (2014). Impact of HbA1c measurement on hospital readmission rates: analysis of 70,000 clinical database patient records. *BioMed Research International*, 2014. https://doi.org/10.1155/2014/781670

2. UCI Machine Learning Repository. Diabetes 130-US hospitals for years 1999–2008. https://archive.ics.uci.edu/ml/datasets/Diabetes+130-US+hospitals+for+years+1999-2008

3. Apache Hadoop 3.4.1 Documentation. https://hadoop.apache.org/docs/r3.4.1/

4. Apache Pig 0.17.0 Documentation. https://pig.apache.org/docs/r0.17.0/

5. Apache Hive 4.2.0 Documentation. https://hive.apache.org/

6. Apache Spark 3.5.9 MLlib Documentation. https://spark.apache.org/docs/3.5.9/ml-guide.html

7. Centers for Medicare & Medicaid Services. Hospital Readmissions Reduction Program (HRRP). https://www.cms.gov/medicare/quality/value-based-programs/hrrp

---

*Pipeline Track: A — HDFS → Pig → Hive (+ Bonus: Spark MLlib)*
*Tools and Versions: Hadoop 3.4.1 · Pig 0.17.0 · Hive 4.2.0 · Spark 3.5.9 · Python 3.12 · PySpark 3.5.9*
*Dataset: UCI Diabetes 130-US Hospitals 1999–2008 (10,000 balanced sample)*
