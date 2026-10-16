import subprocess
import os
import re
import pandas as pd
from pathlib import Path

RESULTS_DIR = Path('/home/manic/project/results')
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

queries = [
    {
        "name": "1. Overall Dataset & Target Distribution",
        "sql": """
        SELECT
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_nbr) AS unique_patients,
            SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS readmitted_under_30_count,
            SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS not_readmitted_under_30_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_under_30_rate_pct
        FROM cse412_readmission.cleaned_encounters_full;
        """
    },
    {
        "name": "2. Readmission Rate by Age Group",
        "sql": """
        SELECT
            age,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY age
        ORDER BY age;
        """
    },
    {
        "name": "3. Readmission Rate by Gender",
        "sql": """
        SELECT
            gender,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY gender
        ORDER BY encounter_count DESC;
        """
    },
    {
        "name": "4. Readmission Rate by Prior Inpatient Admissions",
        "sql": """
        SELECT
            CASE
                WHEN number_inpatient = 0 THEN '0 Prior Inpatient'
                WHEN number_inpatient = 1 THEN '1 Prior Inpatient'
                WHEN number_inpatient = 2 THEN '2 Prior Inpatient'
                WHEN number_inpatient = 3 THEN '3 Prior Inpatient'
                WHEN number_inpatient = 4 THEN '4 Prior Inpatient'
                ELSE '5+ Prior Inpatient'
            END AS prior_inpatient_tier,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY
            CASE
                WHEN number_inpatient = 0 THEN '0 Prior Inpatient'
                WHEN number_inpatient = 1 THEN '1 Prior Inpatient'
                WHEN number_inpatient = 2 THEN '2 Prior Inpatient'
                WHEN number_inpatient = 3 THEN '3 Prior Inpatient'
                WHEN number_inpatient = 4 THEN '4 Prior Inpatient'
                ELSE '5+ Prior Inpatient'
            END
        ORDER BY MIN(number_inpatient);
        """
    },
    {
        "name": "5. Readmission Rate by Number of Diagnoses (Comorbidities)",
        "sql": """
        SELECT
            CASE
                WHEN number_diagnoses <= 3 THEN '1-3 Diagnoses'
                WHEN number_diagnoses <= 6 THEN '4-6 Diagnoses'
                WHEN number_diagnoses <= 8 THEN '7-8 Diagnoses'
                ELSE '9+ Diagnoses'
            END AS comorbidity_tier,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY
            CASE
                WHEN number_diagnoses <= 3 THEN '1-3 Diagnoses'
                WHEN number_diagnoses <= 6 THEN '4-6 Diagnoses'
                WHEN number_diagnoses <= 8 THEN '7-8 Diagnoses'
                ELSE '9+ Diagnoses'
            END
        ORDER BY MIN(number_diagnoses);
        """
    },
    {
        "name": "6. Impact of Medication Changes on Readmission",
        "sql": """
        SELECT
            CASE WHEN change = 'Ch' THEN 'Medication Changed' ELSE 'No Medication Change' END AS medication_status,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY CASE WHEN change = 'Ch' THEN 'Medication Changed' ELSE 'No Medication Change' END;
        """
    },
    {
        "name": "7. Readmission Rate by Insulin Regimen Status",
        "sql": """
        SELECT
            insulin AS insulin_regimen,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY insulin
        ORDER BY readmit_rate_pct DESC;
        """
    },
    {
        "name": "8. Readmission Rate Across Top 8 Medical Specialties",
        "sql": """
        SELECT
            medical_specialty,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        WHERE medical_specialty != 'Unknown'
        GROUP BY medical_specialty
        HAVING COUNT(*) >= 1000
        ORDER BY readmit_rate_pct DESC
        LIMIT 8;
        """
    },
    {
        "name": "9. Readmission Rate by Hospital Length of Stay",
        "sql": """
        SELECT
            CASE
                WHEN time_in_hospital <= 2 THEN '1-2 Days (Short)'
                WHEN time_in_hospital <= 4 THEN '3-4 Days (Moderate)'
                WHEN time_in_hospital <= 7 THEN '5-7 Days (Extended)'
                ELSE '8+ Days (Long-term)'
            END AS stay_duration,
            COUNT(*) AS encounter_count,
            SUM(label) AS readmitted_count,
            ROUND(100.0 * AVG(CAST(label AS DOUBLE)), 2) AS readmit_rate_pct
        FROM cse412_readmission.cleaned_encounters_full
        GROUP BY
            CASE
                WHEN time_in_hospital <= 2 THEN '1-2 Days (Short)'
                WHEN time_in_hospital <= 4 THEN '3-4 Days (Moderate)'
                WHEN time_in_hospital <= 7 THEN '5-7 Days (Extended)'
                ELSE '8+ Days (Long-term)'
            END
        ORDER BY MIN(time_in_hospital);
        """
    }
]

env = os.environ.copy()
env['TERM'] = 'dumb'

full_output = []

print("======================================================================")
print("  CSE412 TRACK A: HIVE WAREHOUSING ANALYTICS ON FULL DATASET (101,766)")
print("======================================================================\n")

for q in queries:
    title = q["name"]
    sql = f"SET mapreduce.framework.name=local;\nSET hive.exec.mode.local.auto=true;\n{q['sql'].strip()}"
    print(f"--- Running Query: {title} ---")
    
    cmd = [
        'beeline', '-u', 'jdbc:hive2://localhost:10000/default', '-n', 'manic',
        '--outputformat=tsv2',
        '-e', sql
    ]
    
    proc = subprocess.run(cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = proc.stdout
    
    lines = [line.strip() for line in out.splitlines() if line.strip() and not line.startswith('SLF4J:') and not line.startswith('Beeline version') and not line.startswith('Connecting to') and not line.startswith('Connected to') and not line.startswith('Transaction manager') and not line.startswith('Closing:')]
    
    # filter out log info lines from Hive
    clean_lines = [l for l in lines if not l.startswith('INFO  :') and not l.startswith('WARN  :')]
    
    result_text = "\n".join(clean_lines)
    print(result_text)
    print("\n")
    
    full_output.append(f"### {title}\n```tsv\n{result_text}\n```\n")

report_path = RESULTS_DIR / 'full_dataset_hive_analytics.md'
report_path.write_text("# Full Dataset (101,766 Encounters) Hive Analytics Report\n\n" + "\n".join(full_output), encoding='utf-8')
print(f"Analytics report saved to: {report_path}")
