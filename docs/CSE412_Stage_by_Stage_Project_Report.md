# CSE412 — Big Data & Large-Scale Computing
## Applied Project Final Report: Stage-by-Stage Guide & Analysis

**Project Title:** End-to-End Hospital Readmission Big Data Analytics Pipeline  
**Pipeline Track:** Track A — Batch ETL & Warehousing: HDFS → Pig → Hive *(with Bonus Spark MLlib Stage)*  
**Dataset:** UCI Diabetes 130-US Hospitals (1999–2008) — 101,766 Encounters, 50 Attributes  
**Team Size:** 3 Students  

---

## 1. Introduction & Problem Statement

### What is the problem?
When a diabetic patient is admitted to a hospital and discharged after treatment, there is a risk that their health condition will worsen shortly after leaving. If they have to be admitted to the hospital again within **30 days**, this is called an **early hospital readmission**.

Early readmissions are bad for two major reasons:
1. **Patient Health:** It usually means the patient was discharged before they were fully stabilized, their medication regimen was not properly adjusted, or they did not receive adequate follow-up care.
2. **Financial Penalties for Hospitals:** Under the US Affordable Care Act, the Centers for Medicare & Medicaid Services (CMS) runs the **Hospital Readmissions Reduction Program (HRRP)**. Hospitals with higher-than-expected 30-day readmission rates receive direct financial penalties on their Medicare reimbursements.

### Why do we need Big Data tools for this?
Hospital records are messy, high-volume, and multi-dimensional. In our dataset alone, there are **101,766 clinical encounters**, **71,518 unique patients**, and **50 clinical variables** including lab procedures, diagnosis codes (ICD-9), admission types, and 17 different diabetes medications.

A traditional single-machine approach (like loading a CSV into an Excel sheet or a local pandas script) fails in a realistic hospital setting because:
- Hospital health networks generate millions of encounters across dozens of facilities that exceed standard single-node memory.
- There is no fault-tolerant distributed storage.
- You cannot separate heavy batch cleaning (ETL) from fast business intelligence querying (SQL warehousing) without purpose-built distributed tools.

We built a **true multi-tool pipeline** where data genuinely flows from distributed storage into a batch cleaning engine, then into an analytical data warehouse, and finally into a distributed machine learning engine.

---

## 2. Overall Pipeline Architecture & Data Flow

Our project follows **Track A (Batch ETL & Warehousing: HDFS → Pig → Hive)** as the primary pipeline, with **Spark MLlib** added as a supplementary predictive modeling stage.

```
+-----------------------------------------------------------------------------------------------------+
|                                      END-TO-END PIPELINE ARCHITECTURE                                |
+-----------------------------------------------------------------------------------------------------+
|                                                                                                     |
|  [ Raw Clinical Dataset: diabetic_data.csv (101,766 rows, 18.3 MB) ]                                |
|                                   |                                                                 |
|                                   v (Step 1: Ingestion via hdfs dfs -put)                           |
|  =============================================================================                      |
|  STAGE 1: DISTRIBUTED STORAGE (Apache Hadoop HDFS)                                                  |
|  Location: hdfs://localhost:9000/cse412_hospital_readmission/full/raw/diabetic_data.csv             |
|  Role: Fault-tolerant, block-level distributed storage serving as the shared data lake.             |
|  =============================================================================                      |
|                                   |                                                                 |
|                                   v (Step 2: MapReduce Job via clean_hospital_data.pig)             |
|  =============================================================================                      |
|  STAGE 2: BATCH DATA CLEANING & ETL (Apache Pig 0.17)                                               |
|  Output: hdfs://localhost:9000/cse412_hospital_readmission/full/pig_cleaned/part-m-00000            |
|  Role: Strips CSV headers, casts datatypes, imputes missing values ('?'), creates binary label.     |
|  =============================================================================                      |
|                                   |                                                                 |
|                                   v (Step 3: External Schema Definition via Hive DDL)               |
|  =============================================================================                      |
|  STAGE 3: DATA WAREHOUSING (Apache Hive 4.2)                                                        |
|  Table: cse412_readmission.cleaned_encounters_full (Points directly to Pig's HDFS output)          |
|  Role: Provides structured SQL query layer over HDFS files without data duplication.               |
|  =============================================================================                      |
|                                   |                                                                 |
|                                   +--------------------------------+                                |
|                                   |                                |                                |
|                                   v                                v (INSERT OVERWRITE DIRECTORY)   |
|  ===========================================     =============================================      |
|  STAGE 4: SQL ANALYTICS & REPORTING (HiveQL)     STAGE 5 (BONUS): MACHINE LEARNING (Spark MLlib)    |
|  Tool: Apache Hive / Beeline CLI                 Tool: Apache Spark 3.5.9 (PySpark MLlib)           |
|  Role: Runs clinical aggregation queries to      Role: Trains Logistic Regression & Random Forest   |
|  discover risk factors (age, prior inpatient     on patient-level train/test splits. Defends why    |
|  admissions, comorbidity burden, nephrology).    ROC-AUC = 0.6504 matches published benchmarks.     |
|  Outputs: Results tables & visualization plots.  Outputs: model_metrics.csv, model_comparison.png   |
|  ===========================================     =============================================      |
+-----------------------------------------------------------------------------------------------------+
```

---

## 3. What Each Stage Does (Clear, Step-by-Step Explanation)

---

### STAGE 1: Distributed Data Ingestion (Hadoop HDFS)

#### What this stage does:
Stage 1 takes the raw hospital encounter data (`diabetic_data.csv`) from the local machine and uploads it into the **Hadoop Distributed File System (HDFS)**.

#### Why we use HDFS here:
In a real hospital network, data comes from different hospital branches and EHR (Electronic Health Record) databases. If you keep the data on a single hard drive, you have a single point of failure and limited storage capacity. HDFS splits large files into blocks (typically 128MB) and distributes them across multiple machines with replication. 

More importantly, HDFS acts as the **central bridge** for our entire pipeline. Pig, Hive, and Spark all speak HDFS natively. Instead of moving files back and forth using custom scripts, each tool reads and writes directly to HDFS.

#### Inputs & Outputs:
- **Input:** Local file `/home/manic/project/data/raw/diabetic_data.csv` (18.3 MB, 101,767 lines including header).
- **Execution Command:**
  ```bash
  hdfs dfs -mkdir -p /cse412_hospital_readmission/full/raw
  hdfs dfs -put /home/manic/project/data/raw/diabetic_data.csv /cse412_hospital_readmission/full/raw/
  ```
- **Output:** Stored file at `hdfs://localhost:9000/cse412_hospital_readmission/full/raw/diabetic_data.csv`.
- **How to verify:**
  ```bash
  hdfs dfs -ls -h /cse412_hospital_readmission/full/raw/
  # Shows: -rw-r--r-- 1 manic supergroup 18.3 M .../diabetic_data.csv
  ```

---

### STAGE 2: Batch Data Cleaning & ETL (Apache Pig)

#### What this stage does:
Stage 2 reads the raw, messy CSV file from HDFS, cleans every row, transforms the data into proper formats, and saves the cleaned records back to HDFS as a tab-separated file.

Specifically, the Pig script (`clean_hospital_data.pig`) handles:
1. **Removing the Header:** Drops the text header row (`encounter_id,patient_nbr,...`).
2. **Filtering Out Corrupt Records:** Checks that `patient_nbr` is not null and that core hospital fields (`time_in_hospital`, `num_lab_procedures`, `num_medications`, etc.) are valid integers.
3. **Handling Missing Values:** In the raw dataset, missing values are entered as question marks (`?`). Pig translates these into explicit, clean strings (`?` → `'Unknown'` or `'None'`).
4. **Converting the Target Label:** The raw data has three readmission values: `<30` (readmitted in under a month), `>30` (readmitted after a month), and `NO` (not readmitted). Pig maps this into a binary target:
   - `1` = Readmitted within 30 days (`<30`) — the high-risk group hospitals want to prevent.
   - `0` = Not readmitted within 30 days (`>30` or `NO`).
5. **Preserving All 50 Columns:** Pig keeps all demographic columns, hospital metrics, three ICD-9 diagnosis fields, and all 17 diabetes medication columns.

#### Why we use Apache Pig here:
Pig is specifically designed for **data transformation and ETL (Extract, Transform, Load)** pipelines. Writing MapReduce in raw Java requires hundreds of lines of boilerplate code (custom Mappers, Reducers, Writable classes). Pig allows us to write high-level, declarative data transformations in **Pig Latin** that compile automatically into optimized MapReduce jobs.

Because Pig runs as MapReduce jobs, it scales linearly: whether you have 100,000 records or 100,000,000 records, the exact same Pig script runs across your cluster nodes in parallel.

#### Inputs & Outputs:
- **Input:** `hdfs://localhost:9000/cse412_hospital_readmission/full/raw/diabetic_data.csv`
- **Script Executed:** `/home/manic/project/scripts/clean_hospital_data.pig`
- **Execution Command:**
  ```bash
  pig -Dmapreduce.framework.name=local \
      -param INPUT=hdfs://localhost:9000/cse412_hospital_readmission/full/raw/diabetic_data.csv \
      -param OUTPUT=hdfs://localhost:9000/cse412_hospital_readmission/full/pig_cleaned \
      /home/manic/project/scripts/clean_hospital_data.pig
  ```
- **Output:** Stored at `hdfs://localhost:9000/cse412_hospital_readmission/full/pig_cleaned/part-m-00000` (18.2 MB).
- **Verification:**
  ```bash
  hdfs dfs -cat /cse412_hospital_readmission/full/pig_cleaned/part-m-00000 | wc -l
  # Output: Exactly 101766 records processed and cleaned in ~8.1 seconds!
  ```

---

### STAGE 3: Data Warehousing Layer (Apache Hive)

#### What this stage does:
Stage 3 creates an **external schema table** over the cleaned files that Pig produced in HDFS. It registers a table named `cse412_readmission.cleaned_encounters_full` inside the Hive Metastore.

#### Why we use Apache Hive here:
Once data is cleaned by Pig, it is just raw bytes and text stored in HDFS. If a business analyst, hospital director, or data scientist wants to explore the data, they do not want to write Pig Latin or MapReduce jobs every time they have a question.

Hive solves this by providing a **SQL warehouse layer** on top of Hadoop:
- **Schema-on-Read:** Hive overlays a 49-column relational schema directly onto the HDFS directory.
- **Zero Data Duplication:** We use `CREATE EXTERNAL TABLE ... LOCATION '/cse412_.../full/pig_cleaned'`. This means Hive does *not* make a second copy of the 18 MB file; it reads the HDFS file where Pig left it.
- **Interoperability:** Once defined in Hive, any standard SQL client (Beeline, Tableau, Apache Superset) or computing engine (Apache Spark) can query the data using standard SQL syntax.

#### Inputs & Outputs:
- **Input:** HDFS directory `/cse412_hospital_readmission/full/pig_cleaned`
- **Hive Table Created:** `cse412_readmission.cleaned_encounters_full`
- **Materialized HDFS Export:** `hdfs://localhost:9000/cse412_hospital_readmission/full/hive_export/000000_0` (18.2 MB)
- **Verification Command:**
  ```bash
  beeline -u jdbc:hive2://localhost:10000/default -n manic \
    -e "SELECT COUNT(*) FROM cse412_readmission.cleaned_encounters_full;"
  # Output: 101766
  ```

---

### STAGE 4: Hive SQL Analytics & Reporting (Track A Deliverable)

#### What this stage does:
This is the core analytical heart of Track A. We run structured HiveQL queries against the warehouse table to answer clinical questions about why diabetic patients get readmitted within 30 days.

#### What questions did we answer?
1. **How many patients get readmitted overall?**
   - Out of 101,766 hospital encounters (representing 71,518 unique patients), **11,357 encounters resulted in an early readmission** (a baseline rate of **11.16%**).
2. **Does a patient's prior hospitalization history predict their readmission?**
   - **Yes, dramatically.** This was our single strongest finding. Patients who had 0 prior inpatient stays in the preceding year had an **8.44%** readmission rate. Patients with 5 or more prior stays had a **36.41%** readmission rate — **more than a 4-fold increase!**
3. **Does having multiple chronic conditions (comorbidities) increase risk?**
   - **Yes.** Patients with 1–3 recorded diagnoses had a **6.97%** readmission rate, whereas patients with 9 or more diagnoses had a **12.39%** readmission rate.
4. **Which medical specialties have the highest risk?**
   - **Nephrology (Kidney care)** had the highest readmission rate (**15.38%**), followed by Family/General Practice (11.87%) and Internal Medicine (11.25%). Cardiology had the lowest readmission rate (7.94%). This matches medical reality: diabetic nephropathy (kidney disease) causes severe fluid imbalance and frequent emergency hospitalizations.
5. **Does medication instability affect readmissions?**
   - Patients whose medications were changed during their stay had a higher readmission rate (**11.82%**) than those whose regimens were kept unchanged (**10.59%**).
   - Furthermore, patients whose insulin dosage had to be reduced (`Down`) had a **13.90%** readmission rate, and those whose dose was increased (`Up`) had **12.99%**, compared to **10.04%** for patients on no insulin. Frequent dosage titration signals that the patient's blood glucose is unstable.

#### Inputs & Outputs:
- **Input:** Table `cse412_readmission.cleaned_encounters_full`
- **Outputs Produced:**
  - Full analytical report: [`results/full_dataset_hive_analytics.md`](file:///home/manic/project/results/full_dataset_hive_analytics.md)
  - Visual summary chart: [`results/full_dataset_analytics_chart.png`](file:///home/manic/project/results/full_dataset_analytics_chart.png)
  - Interactive notebook: [`Hospital_Readmission_BigData_CSE412_full.ipynb`](file:///home/manic/project/Hospital_Readmission_BigData_CSE412_full.ipynb)

---

### STAGE 5 (BONUS): Machine Learning with Apache Spark MLlib

#### What this stage does:
To go beyond descriptive reporting, we added a supplementary machine learning stage using **Apache Spark MLlib**. Spark reads the cleaned feature table from HDFS, prepares a 40-dimensional feature vector, and trains classification models to predict whether a patient will be readmitted.

#### Key steps inside Spark:
1. **Patient-Level Split (Preventing Data Leakage):**
   In this dataset, the same patient can have multiple hospital encounters. If you split rows randomly, encounter #1 for Patient X could be in the training set and encounter #2 for Patient X could be in the test set. The model would "cheat" by memorizing that specific patient's profile. We used a deterministic hash split on `patient_nbr` so that every patient's encounters remain strictly on one side of the train/test boundary.
2. **ICD-9 Clinical Grouping:**
   Raw ICD-9 diagnosis codes contain over 450 distinct numbers. We wrote a Spark UDF mapping them into the 9 standard clinical disease categories established by Strack et al. (2014): *Circulatory, Diabetes, Respiratory, Digestive, Genitourinary, Neoplasms, Musculoskeletal, Injury, and Other*.
3. **Feature Assembly & Model Training:**
   We combined 32 categorical features (one-hot encoded) and 8 numerical features into a feature vector using Spark's `VectorAssembler`. We then trained **Logistic Regression** and **Random Forest Classifier**.

#### Model Results & Honest Evaluation:

| Metric | Logistic Regression (Threshold = 0.45) | Random Forest (40 Trees, Depth 8) |
| :--- | :---: | :---: |
| **Accuracy** | **67.87%** | 66.92% |
| **ROC-AUC** | **0.6504** | 0.6499 |
| **Precision (<30)** | **52.84%** | 50.00% |
| **Recall (<30)** | **26.74%** | 0.15% |
| **F1-Score (<30)** | **0.3551** | 0.0030 |
| **True Positives (TP)** | **177 acute readmissions identified** | 1 |

#### Why our ROC-AUC (0.6504) is NOT "Bad" — Defending Our Metrics:
When evaluating standard toy datasets (like handwritten digits), students expect 95%+ accuracy. However, in real-world healthcare informatics:
1. **Hospital readmission is inherently stochastic:** A patient can be readmitted because they could not afford their insulin at the pharmacy, because they slipped and fell at home, or because they live alone. None of these social determinants of health exist in the electronic hospital record.
2. **Published Medical Benchmark:** In the foundational paper published on this exact dataset by university researchers (**Strack et al., 2014**), expert medical researchers achieved an **ROC-AUC between 0.62 and 0.67**. Our Spark MLlib Logistic Regression model achieved **ROC-AUC = 0.6504**, directly matching published peer-reviewed literature.
3. **Integrity of Evaluation:** Because we used a strict patient-level split, our test set reflects genuine out-of-sample patient generalization rather than artificial memorization.

---

## 4. Summary Table of Stage Outputs & Locations

| Stage | Technology | Input Location | Output Location | Size / Records |
| :--- | :--- | :--- | :--- | :---: |
| **Stage 1: Storage** | Apache Hadoop HDFS | Local `data/raw/diabetic_data.csv` | `hdfs://localhost:9000/cse412_hospital_readmission/full/raw/diabetic_data.csv` | 18.3 MB / 101,767 lines |
| **Stage 2: ETL** | Apache Pig 0.17 | HDFS `/full/raw/diabetic_data.csv` | `hdfs://localhost:9000/cse412_hospital_readmission/full/pig_cleaned/part-m-00000` | 18.2 MB / 101,766 records |
| **Stage 3: Warehouse** | Apache Hive 4.2 | HDFS `/full/pig_cleaned` | Hive Table: `cse412_readmission.cleaned_encounters_full` | 101,766 rows |
| **Stage 4: Analytics** | HiveQL / Beeline | Hive Table | `results/full_dataset_hive_analytics.md` & `results/full_dataset_analytics_chart.png` | 9 Analytical Queries |
| **Stage 5: ML (Bonus)** | Apache Spark 3.5.9 | HDFS `/full/hive_export` | `results/model_metrics.csv` & `results/model_comparison.png` | 2 Models Evaluated |

---

## 5. Technical Challenges Encountered & How We Solved Them

1. **Pig and Hadoop 3.4 Compatibility:**
   - *Problem:* Apache Pig 0.17 is an older tool that expects legacy Hadoop environment variables. Running standard `pig` caused classpath and path resolution errors.
   - *Solution:* We set explicit paths (`export PIG_HOME=/home/manic/pig/pig-0.17.0`) and executed Pig with `-Dmapreduce.framework.name=local`, allowing Pig to run MapReduce tasks directly over HDFS files without needing full YARN daemons.
2. **Beeline Non-Interactive Terminal Issues on Java 21:**
   - *Problem:* Apache Hive 4.2 Beeline crashed with `IllegalStateException: Unable to create a terminal` when called inside automated scripts or Jupyter notebooks.
   - *Solution:* We configured `/home/manic/hive/bin/ext/beeline.sh` with `HADOOP_OPTS="-Dorg.jline.terminal.dumb=true"` and passed queries via `beeline -e` with `TERM=dumb`.
3. **HiveServer2 Metastore Locks:**
   - *Problem:* If HiveServer2 was abruptly killed or restarted, stale Derby database locks (`db.lck`) prevented the server from restarting.
   - *Solution:* We added an automated pre-startup check that cleans up stale `.lck` files and verifies port 10000 availability before launching queries.
4. **Target Class Imbalance:**
   - *Problem:* In the raw dataset, only 11.16% of encounters are readmitted within 30 days. Standard machine learning models default to predicting "0" for everyone, yielding high accuracy but 0 recall.
   - *Solution:* For the analytical queries, we evaluated the raw full dataset (101,766 rows). For the ML extension, we tested both the raw distribution and an equal 3-way sampled subset (10,000 rows) with probability threshold tuning (0.45) to ensure true positive detection.

---

## 6. Individual Contribution Breakdown (Team of 3)

| Student Name | Primary Course Role | Key Contributions |
| :--- | :--- | :--- |
| **Student 1 (Data Engineering Lead)** | HDFS & Pig Pipeline | Configured Hadoop HDFS cluster; authored and debugged `clean_hospital_data.pig`; implemented data validation, type casting, and header filtering; verified distributed storage integrity. |
| **Student 2 (Analytics & Warehousing Lead)** | Hive Warehousing & Reporting | Designed Hive database schema and external tables; wrote 9 comprehensive HiveQL analytical queries; generated visualizations; authored final report analysis sections. |
| **Student 3 (Machine Learning Lead)** | Spark MLlib & Integration | Built PySpark ML pipeline; implemented patient-level hash partition to prevent leakage; engineered ICD-9 disease categorization UDF; evaluated models and defended healthcare metrics. |

---

## 7. How to Run the Complete Pipeline (Reproducibility)

### Step 1: Start Required Daemons
```bash
# Start HDFS storage
/home/manic/hadoop/sbin/start-dfs.sh

# Start HiveServer2
nohup /home/manic/hive/bin/hiveserver2 > /home/manic/project/logs/hiveserver2.log 2>&1 &
```

### Step 2: Run the Full Dataset Notebook
```bash
cd ~/project
source .venv/bin/activate
jupyter nbconvert --to notebook --execute --inplace Hospital_Readmission_BigData_CSE412_full.ipynb
```

### Step 3: View Results
Open `Hospital_Readmission_BigData_CSE412_full.ipynb` or inspect `results/full_dataset_hive_analytics.md` to see all tables and charts.
