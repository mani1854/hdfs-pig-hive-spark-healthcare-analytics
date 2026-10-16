# CSE412 — Big Data & Large-Scale Computing
## Progress Check-in 2 — Draft Analytical Results

**Project:** Hospital Readmission Analytics Pipeline
**Check-in Date:** [Week 4]
**Team:** [Student 1] · [Student 2] · [Student 3]

---

## Summary

This check-in demonstrates that the full analytical pipeline is running end-to-end. Apache Hive is loaded with clean data from Pig, we have run multiple analytical queries and gotten meaningful results, and our supplementary Spark MLlib model has been trained and evaluated.

**Status at this check-in:**
- ✅ HDFS ingestion (Stage 1) — complete
- ✅ Apache Pig ETL (Stage 2) — complete
- ✅ Apache Hive warehousing + analytics (Stage 3) — **complete, results below**
- ✅ Apache Spark MLlib model (Stage 4, bonus) — **trained, evaluated, results below**
- 🔲 Final report — being written
- 🔲 Demo preparation — in progress

---

## 1. Apache Hive — Table Setup

After Pig wrote the cleaned 10,000-row dataset to HDFS at `/cse412_hospital_readmission/pig/cleaned/`, we created an external Hive table pointing to that location.

**Database and table creation:**
```sql
CREATE DATABASE IF NOT EXISTS cse412_readmission;

CREATE EXTERNAL TABLE cse412_readmission.cleaned_encounters (
    encounter_id INT,
    patient_nbr BIGINT,
    race STRING,
    gender STRING,
    age STRING,
    time_in_hospital INT,
    num_lab_procedures INT,
    num_procedures INT,
    num_medications INT,
    number_outpatient INT,
    number_emergency INT,
    number_inpatient INT,
    number_diagnoses INT,
    -- ... all 50 columns including 17 medication columns ...
    label INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/cse412_hospital_readmission/pig/cleaned/';
```

**Verification — row count:**
```sql
SELECT COUNT(*) FROM cse412_readmission.cleaned_encounters;
-- Result: 10000
```

All 10,000 rows loaded from Pig output. No manual file copying — Hive reads directly from HDFS. ✅

---

## 2. Hive Analytics Results

We ran 5 analytical queries to understand readmission patterns in the data.

### Query 1: Readmission Rate by Age Group

```sql
SELECT
    age,
    COUNT(*) AS total_encounters,
    SUM(label) AS readmitted_count,
    ROUND(100.0 * SUM(label) / COUNT(*), 2) AS readmission_rate_pct
FROM cse412_readmission.cleaned_encounters
GROUP BY age
ORDER BY age;
```

**Results:**

| Age Group | Total | Readmitted <30 | Rate % |
|-----------|-------|----------------|--------|
| [0-10)    | 42    | 14             | 33.3%  |
| [10-20)   | 113   | 38             | 33.6%  |
| [20-30)   | 254   | 85             | 33.5%  |
| [30-40)   | 423   | 141            | 33.3%  |
| [40-50)   | 861   | 289            | 33.6%  |
| [50-60)   | 1,568 | 521            | 33.2%  |
| [60-70)   | 1,892 | 631            | 33.4%  |
| [70-80)   | 2,443 | 815            | 33.4%  |
| [80-90)   | 1,978 | 659            | 33.3%  |
| [90-100)  | 426   | 141            | 33.1%  |

**What this tells us:** Because we used equal-class sampling, the base rate is uniform at ~33% across all age groups. But the distribution of encounters is clinically meaningful — patients aged 70–80 account for the most hospital visits (2,443 encounters), which makes sense since elderly diabetics have more frequent complications.

---

### Query 2: Readmission by Number of Prior Inpatient Visits

```sql
SELECT
    number_inpatient,
    COUNT(*) AS total_encounters,
    SUM(label) AS readmitted_count,
    ROUND(100.0 * SUM(label) / COUNT(*), 2) AS readmission_rate_pct
FROM cse412_readmission.cleaned_encounters
WHERE number_inpatient <= 5
GROUP BY number_inpatient
ORDER BY number_inpatient;
```

**Results:**

| Prior Inpatient Visits | Total | Readmitted | Rate % |
|------------------------|-------|------------|--------|
| 0 | 6,287 | 1,788 | 28.4% |
| 1 | 1,998 | 724  | 36.2% |
| 2 | 921   | 371  | 40.3% |
| 3 | 415   | 183  | 44.1% |
| 4 | 198   | 97   | 49.0% |
| 5 | 88    | 48   | 54.5% |

**What this tells us:** This is our strongest finding. Patients with zero prior inpatient visits have a 28.4% readmission rate. Patients with 5 prior inpatient visits have a 54.5% readmission rate — nearly double. Each additional prior inpatient visit adds roughly 5–6 percentage points of readmission risk. A hospital could use this single feature as a simple early-warning flag.

---

### Query 3: Readmission by Number of Diagnoses (Comorbidities)

```sql
SELECT
    number_diagnoses,
    COUNT(*) AS total_encounters,
    SUM(label) AS readmitted_count,
    ROUND(100.0 * SUM(label) / COUNT(*), 2) AS readmission_rate_pct
FROM cse412_readmission.cleaned_encounters
GROUP BY number_diagnoses
ORDER BY number_diagnoses;
```

**Key findings:**

| Diagnoses Count | Total | Rate % |
|----------------|-------|--------|
| 1–3            | ~400  | ~26%   |
| 4–6            | ~2,200| ~30%   |
| 7–9            | ~5,800| ~34%   |
| 9 (max)        | ~1,400| ~36%   |

**What this tells us:** Patients with more diagnoses (multiple conditions at once) are consistently at higher readmission risk. Comorbidity burden is a well-known risk factor in medical literature, and our Hive data confirms this in our dataset.

---

### Query 4: Impact of Medication Changes

```sql
SELECT
    change,
    COUNT(*) AS total_encounters,
    SUM(label) AS readmitted_count,
    ROUND(100.0 * SUM(label) / COUNT(*), 2) AS readmission_rate_pct
FROM cse412_readmission.cleaned_encounters
GROUP BY change;
```

**Results:**

| Medication Change | Total | Readmitted | Rate % |
|------------------|-------|------------|--------|
| No change (Ch=No) | 5,128 | 1,595 | 31.1% |
| Change made (Ch=Ch) | 4,872 | 1,739 | 35.7% |

**What this tells us:** When a patient's medications were changed during their hospital stay, their readmission rate is 4.6 percentage points higher than patients whose medications stayed the same. This suggests that medication changes during a visit signal an unstable or complicated condition that may not resolve fully before discharge.

---

### Query 5: Readmission by Insulin Therapy

```sql
SELECT
    insulin,
    COUNT(*) AS total_encounters,
    SUM(label) AS readmitted_count,
    ROUND(100.0 * SUM(label) / COUNT(*), 2) AS readmission_rate_pct
FROM cse412_readmission.cleaned_encounters
GROUP BY insulin
ORDER BY readmission_rate_pct DESC;
```

**Results:**

| Insulin Status | Total | Readmitted | Rate % |
|---------------|-------|------------|--------|
| Up (dose increased) | 1,204 | 462 | 38.4% |
| Down (dose decreased) | 453 | 168 | 37.1% |
| Steady (no change) | 3,891 | 1,351 | 34.7% |
| No insulin | 4,452 | 1,353 | 30.4% |

**What this tells us:** Patients who needed insulin dose adjustments (either up or down) during their stay had higher readmission rates than those on steady insulin or no insulin at all. Again, this likely reflects condition instability — if your insulin dosing needs to change mid-stay, your glucose control is not stable.

---

## 3. Spark MLlib — Initial Model Results

We connected Spark to the Hive-cleaned data and trained two classification models.

**Feature set:** All available numerical and categorical columns (50 features after encoding). ICD-9 diagnosis codes were mapped to clinical categories (Circulatory, Diabetes, Respiratory, etc.) following Strack et al. (2014) methodology.

**Train/test split:** Patient-level hash split — 7,999 training rows, 2,001 test rows. Each patient appears in only one split to prevent data leakage.

**Results:**

| Model | Accuracy | ROC-AUC | Recall (<30) | TP |
|-------|----------|---------|--------------|-----|
| Logistic Regression | 67.9% | **0.6504** | 26.7% | 177 |
| Random Forest | 66.9% | 0.6499 | 0.15% | 1 |

**Note on Random Forest:** Random Forest essentially collapsed to predicting the majority class (TP=1 out of 662 actual positives). This is a known issue with Random Forest at default threshold=0.5 on imbalanced-ish data with high-dimensional features. Logistic Regression performed better with threshold tuned to 0.45.

**Context for these numbers:** The original Strack et al. (2014) paper achieved ROC-AUC of 0.62–0.67 on this same dataset with medical research expertise. Our LR model at 0.6504 sits within that range, which confirms the pipeline is working correctly and our results are credible.

---

## 4. Current Status and Next Steps

**What works:** The full pipeline runs end-to-end. HDFS → Pig → Hive → Spark. Each stage reads from the previous stage's HDFS output. The data is real and the analytical results are meaningful.

**What is left:**
- Write the full project report (draft in progress)
- Expand Hive analytics with 2 more queries
- Prepare the demo walkthrough
- Update README with final run instructions

---

*CSE412 — Big Data & Large-Scale Computing | Progress Check-in 2*
*Full pipeline running: HDFS → Pig → Hive → Spark MLlib*
*Hive analytics complete. Draft ML results available.*
