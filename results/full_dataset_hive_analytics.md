# Full Dataset (101,766 Encounters) Hive Analytics Report

### 1. Overall Dataset & Target Distribution
```tsv
total_encounters	unique_patients	readmitted_under_30_count	not_readmitted_under_30_count	readmit_under_30_rate_pct
101766	71518	11357	90409	11.16
```

### 2. Readmission Rate by Age Group
```tsv
age	encounter_count	readmitted_count	readmit_rate_pct
[0-10)	161	3	1.86
[10-20)	691	40	5.79
[20-30)	1657	236	14.24
[30-40)	3775	424	11.23
[40-50)	9685	1027	10.6
[50-60)	17256	1668	9.67
[60-70)	22483	2502	11.13
[70-80)	26068	3069	11.77
[80-90)	17197	2078	12.08
[90-100)	2793	310	11.1
```

### 3. Readmission Rate by Gender
```tsv
gender	encounter_count	readmitted_count	readmit_rate_pct
Female	54708	6152	11.25
Male	47055	5205	11.06
Unknown/Invalid	3	0	0.0
```

### 4. Readmission Rate by Prior Inpatient Admissions
```tsv
prior_inpatient_tier	encounter_count	readmitted_count	readmit_rate_pct
0 Prior Inpatient	67630	5706	8.44
1 Prior Inpatient	19521	2523	12.92
2 Prior Inpatient	7566	1319	17.43
3 Prior Inpatient	3411	692	20.29
4 Prior Inpatient	1622	383	23.61
5+ Prior Inpatient	2016	734	36.41
```

### 5. Readmission Rate by Number of Diagnoses (Comorbidities)
```tsv
comorbidity_tier	encounter_count	readmitted_count	readmit_rate_pct
1-3 Diagnoses	4077	284	6.97
4-6 Diagnoses	27091	2558	9.44
7-8 Diagnoses	21009	2373	11.3
9+ Diagnoses	49589	6142	12.39
```

### 6. Impact of Medication Changes on Readmission
```tsv
medication_status	encounter_count	readmitted_count	readmit_rate_pct
Medication Changed	47011	5558	11.82
No Medication Change	54755	5799	10.59
```

### 7. Readmission Rate by Insulin Regimen Status
```tsv
insulin_regimen	encounter_count	readmitted_count	readmit_rate_pct
Down	12218	1698	13.9
Up	11316	1470	12.99
Steady	30849	3433	11.13
No	47383	4756	10.04
```

### 8. Readmission Rate Across Top 8 Medical Specialties
```tsv
medical_specialty	encounter_count	readmitted_count	readmit_rate_pct
Nephrology	1613	248	15.38
Family/GeneralPractice	7440	883	11.87
InternalMedicine	14635	1646	11.25
Emergency/Trauma	7565	846	11.18
Surgery-General	3099	342	11.04
Orthopedics	1400	151	10.79
Radiologist	1140	103	9.04
Cardiology	5352	425	7.94
```

### 9. Readmission Rate by Hospital Length of Stay
```tsv
stay_duration	encounter_count	readmitted_count	readmit_rate_pct
1-2 Days (Short)	31432	2874	9.14
3-4 Days (Moderate)	31680	3538	11.17
5-7 Days (Extended)	23364	2900	12.41
8+ Days (Long-term)	15290	2045	13.37
```
