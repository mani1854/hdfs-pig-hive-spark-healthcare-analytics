# CSE412 — Big Data & Large-Scale Computing
## Demo Script — 10-Minute Live Walkthrough

**Project:** Hospital Readmission Analytics Pipeline

> **Before starting:** Make sure all services are running.
> ```bash
> # Start HDFS
> /home/manic/hadoop/sbin/start-dfs.sh
>
> # Start HiveServer2 (wait ~45 seconds before using)
> nohup /home/manic/hive/bin/hiveserver2 > /home/manic/project/logs/hiveserver2.log 2>&1 &
>
> # Verify
> jps
> # Should show: NameNode, DataNode, SecondaryNameNode, RunJar
> ```

---

## Timeline Overview

| Time | Stage | What to show |
|------|-------|-------------|
| 0:00–1:30 | Intro & Architecture | Problem + pipeline diagram |
| 1:30–3:00 | Stage 1: HDFS | Dataset in HDFS |
| 3:00–4:30 | Stage 2: Pig ETL | Pig script + cleaned output |
| 4:30–6:30 | Stage 3: Hive | Table + analytics queries |
| 6:30–8:30 | Stage 4: Spark MLlib (bonus) | Model training + results |
| 8:30–9:30 | Results & Interpretation | Metrics table, what they mean |
| 9:30–10:00 | Wrap-up | Summary + tool justification |

---

## 0:00 – 1:30 | Introduction & Architecture

**What to say:**
> "Our project is about predicting hospital readmissions for diabetic patients. When a patient gets discharged and comes back to the hospital within 30 days, that's called an early readmission — and hospitals are actually financially penalized for having too many of them under a federal program called HRRP.
>
> We built a big data pipeline that analyzes a real dataset of 130 US hospitals over 10 years. Our pipeline has three main stages: we use HDFS for storage, Apache Pig for data cleaning, and Apache Hive for analytics. Then we added Apache Spark MLlib as a bonus stage to run machine learning on top of the clean data."

**What to show on screen:**
- Draw or display the pipeline diagram:
  ```
  Raw CSV  →  HDFS  →  Pig ETL  →  Hive Analytics  →  [Bonus] Spark MLlib
  ```
- Show the project README briefly

---

## 1:30 – 3:00 | Stage 1: HDFS Ingestion

**What to say:**
> "The data starts as a CSV file — the UCI Diabetes dataset, 101,766 rows and 50 columns. We created a balanced 10,000-row sample with equal numbers of readmitted and not-readmitted patients, then uploaded it to HDFS."

**Run these commands:**
```bash
# Show the dataset is in HDFS
hdfs dfs -ls /cse412_hospital_readmission/raw/

# Show the file size and first few lines
hdfs dfs -cat /cse412_hospital_readmission/raw/diabetes_sample.csv | head -5
```

**Expected output:**
```
Found 1 items
-rw-r--r--  1 manic supergroup  1773224  /cse412_hospital_readmission/raw/diabetes_sample.csv

encounter_id,patient_nbr,race,gender,age,...,readmitted
2278392,8222157,Caucasian,Female,[0-10),...,NO
```

**What to say:**
> "1.7 MB of hospital data sitting in HDFS — distributed file storage that Pig and Hive can both access. In a real deployment this would be spread across many machines."

---

## 3:00 – 4:30 | Stage 2: Apache Pig ETL

**What to say:**
> "Next, Apache Pig reads this file from HDFS, cleans it, and writes the result back. Pig is good for this because it runs as MapReduce jobs — so if we had terabytes of data, we'd just add more machines and the same script would work."

**Show the Pig script:**
```bash
cat /home/manic/project/scripts/clean_hospital_data.pig
```

**Point out the key parts:**
- The `LOAD` statement reading from HDFS
- The `FILTER` removing the CSV header and invalid rows
- The label mapping: `readmitted == '<30' ? 1 : 0`
- The `STORE` writing back to HDFS

**Show the Pig output:**
```bash
# Show the cleaned output is in HDFS
hdfs dfs -ls /cse412_hospital_readmission/pig/cleaned/

# Show row count — should be 10,000
hdfs dfs -cat /cse412_hospital_readmission/pig/cleaned/part-m-00000 | wc -l
```

**What to say:**
> "The `_SUCCESS` file is Pig's way of saying the job completed without errors. We have 10,000 cleaned rows ready for Hive."

---

## 4:30 – 6:30 | Stage 3: Apache Hive Analytics

**What to say:**
> "This is the core of our Track A pipeline. Hive creates an external table that points directly at the HDFS location where Pig wrote its output — no data copying. Then we can run SQL queries against it."

**Open Jupyter notebook OR show the Hive results directly:**
```bash
# Connect to Hive and run a quick count
beeline -u jdbc:hive2://localhost:10000/default -n manic \
  -e "SELECT COUNT(*) FROM cse412_readmission.cleaned_encounters;"
```

**Show the most impactful analytical result — prior inpatient visits:**
```sql
SELECT number_inpatient,
       COUNT(*) AS total,
       SUM(label) AS readmitted,
       ROUND(100.0 * SUM(label)/COUNT(*), 1) AS rate_pct
FROM cse412_readmission.cleaned_encounters
WHERE number_inpatient <= 5
GROUP BY number_inpatient
ORDER BY number_inpatient;
```

**What to say while showing results:**
> "Look at this — patients with zero prior inpatient visits in the past year have a 28% readmission rate. Patients with 5 prior inpatient visits have a 54% readmission rate — almost double. This is a clinically meaningful pattern that a hospital could actually act on: if a diabetic patient has had 3 or more prior inpatient admissions this year, flag them for intensive follow-up after discharge."

**Also show one more quick query — medication changes:**
```sql
SELECT change,
       COUNT(*) AS total,
       ROUND(100.0 * SUM(label)/COUNT(*), 1) AS rate_pct
FROM cse412_readmission.cleaned_encounters
GROUP BY change;
```

> "Patients whose medications were changed during their stay had a 4.6 percentage point higher readmission rate. That's another actionable signal from Hive."

---

## 6:30 – 8:30 | Stage 4 (Bonus): Spark MLlib

**What to say:**
> "After the Hive analytics, we added a bonus machine learning stage. Spark reads the Hive-cleaned data and trains classification models to predict which patients will be readmitted within 30 days."

**Open the Jupyter notebook** `Hospital_Readmission_BigData_CSE412_small.ipynb` — scroll to the Spark ML section.

**Point out:**
1. The patient-level hash split (prevents same patient appearing in both train/test)
2. The feature pipeline (StringIndexer → OneHotEncoder → VectorAssembler)
3. The ICD-9 diagnosis categorization (Circulatory, Diabetes, Respiratory, etc.)

**Show the results cell or the model metrics CSV:**
```bash
cat /home/manic/project/results/model_metrics.csv
```

**Show the chart:**
```bash
# Display the model comparison chart
eog /home/manic/project/results/model_comparison.png
# OR open from the notebook output
```

---

## 8:30 – 9:30 | Results & Interpretation

**What to say:**
> "Our Logistic Regression model got a ROC-AUC of 0.6504. The first time we saw this, we thought it was bad — 67% accuracy sounds low. But here's the context: the researchers who published this dataset in a peer-reviewed medical paper got ROC-AUC of 0.62 to 0.67. We matched the benchmark of actual published medical research.
>
> Predicting readmissions is genuinely hard. Whether a patient gets readmitted often depends on things like: can they afford their medications? Do they live alone? Do they have someone to drive them to follow-up appointments? None of that is in the clinical data. What is impressive is that our pipeline identified patterns — prior admissions, comorbidities, medication instability — that do correlate with readmission risk.
>
> Also worth noting: we used a patient-level train/test split, which is the medically correct way to evaluate this. If the same patient was in both training and test sets, the model would learn that specific person's records and score artificially high. Our more rigorous split gives honest numbers."

**Show the model metrics table:**
| Model | Accuracy | ROC-AUC | TP (caught) |
|-------|----------|---------|-------------|
| Logistic Regression | 67.9% | 0.6504 | 177 |
| Random Forest | 66.9% | 0.6499 | 1 |

---

## 9:30 – 10:00 | Wrap-Up

**What to say:**
> "To summarize: we built a real, end-to-end big data pipeline. Data flows from HDFS through Pig into Hive, and optionally into Spark. Each tool does what it's actually designed for — HDFS stores data at scale, Pig handles messy ETL as MapReduce jobs, Hive gives us a SQL analytics layer, and Spark trains distributed ML models. The same architecture that runs on 10,000 rows today would scale to 10 million rows by adding hardware — that's the whole point of building this on a distributed stack instead of just using pandas on a laptop."

**Final questions to prepare for:**

**Q: Why did you use equal-class sampling instead of the full 101,766 rows?**
> "Two reasons: first, equal sampling gives us a balanced binary problem, which makes model training more meaningful. Second, running on 10,000 rows keeps demo time reasonable while the architecture is identical to what you'd use on the full dataset — just add DataNodes."

**Q: Why is Random Forest so much worse than Logistic Regression?**
> "Random Forest at default threshold=0.5 tends to predict the majority class when classes aren't perfectly balanced. With 33% positive class rate, it collapsed to predicting 'not readmitted' almost every time. With more hyperparameter tuning and threshold adjustment, RF would perform comparably. We left this honest in our results."

**Q: Why use Hive instead of just querying with Spark SQL?**
> "Hive represents a different layer in the stack — it's the persistent warehouse layer that any tool (Spark, Pig, standard SQL clients) can query. In a real hospital setting, the analytics team would be querying Hive tables with their SQL tools, not running Spark sessions. Our Track A pipeline puts Hive in its proper role as the analytical warehouse."

**Q: Is this pipeline production-ready?**
> "The architecture is production-ready in terms of design. For production use, we'd add: YARN resource management for multi-user workloads, data partitioning in Hive for faster queries on larger datasets, and automated pipeline orchestration with Apache Airflow or Oozie."

---

## Pre-Demo Checklist

Before starting the demo, verify:

- [ ] `jps` shows NameNode, DataNode, SecondaryNameNode, RunJar (HiveServer2)
- [ ] `hdfs dfs -ls /cse412_hospital_readmission/raw/` returns the CSV
- [ ] `hdfs dfs -ls /cse412_hospital_readmission/pig/cleaned/` returns `_SUCCESS` and `part-m-00000`
- [ ] Jupyter notebook launches and the kernel connects
- [ ] `/home/manic/project/results/model_metrics.csv` exists
- [ ] `/home/manic/project/results/model_comparison.png` exists
- [ ] HiveServer2 log shows "Started HiveServer2" (wait 45 seconds after launch)

---

*CSE412 — Big Data & Large-Scale Computing | Demo Script*
*Total time: ~10 minutes | Track A: HDFS → Pig → Hive + Bonus Spark MLlib*
