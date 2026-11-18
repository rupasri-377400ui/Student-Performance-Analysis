# Student Performance Analysis Using Apache Spark

**Mini-project report** — Big Data Analytics
Student name / roll no. / institution / date: *(fill in)*

> **Important status note.** The real UCI `student-mat.csv` could not be downloaded in the environment where this project was built. The code and tests were run only on a **synthetic (randomly generated) file**. Section 8 therefore contains a clearly labelled synthetic demonstration and a table to fill in with **your own real run results**. Nothing in the synthetic section is a finding about real students.

## Abstract
This project uses Apache Spark (PySpark DataFrames) to clean, transform and aggregate the UCI Student Performance (Mathematics) dataset. It derives an average of three grades, classifies students by final grade, and summarises final grades by study time, past failures and absences. The work is descriptive: it does not predict marks or establish causes.

## 1. Introduction and problem statement
Student records contain study habits, school and family context, and grades. Reading them row by row makes patterns hard to see. Spark DataFrames allow the same cleaning and aggregation steps to be written once and scaled from a laptop to larger data.

## 2. Objectives and scope
- Load a semicolon-separated CSV into Spark; inspect schema, counts, samples, statistics.
- Check nulls and duplicates; apply documented cleaning.
- Derive `AverageMarks` and `Performance` (Low 0–9, Medium 10–13, High 14–20 on G3).
- Aggregate G3 by `studytime`, `failures`, and absence bands; list top 10 records.
- Write CSV outputs and charts.

**Out of scope:** machine learning, prediction, causal claims, Hadoop/Hive/Pig/MongoDB/cloud. Spark is the only BDA technology.

## 3. Dataset
Source: UCI Machine Learning Repository, *Student Performance* (Cortez & Silva, 2008) — https://archive.ics.uci.edu/dataset/320/student+performance. File used: `student-mat.csv` (Mathematics, expected 395 rows, 33 columns, `;` separated).

Fields used: `G1`, `G2`, `G3` (period and final grades, 0–20), `studytime` (1: <2 h, 2: 2–5 h, 3: 5–10 h, 4: >10 h weekly), `failures` (past class failures), `absences`; `school`, `sex`, `age` appear in the top-10 table.

Limitations: one subject, two Portuguese schools, a limited number of students, self-reported behaviour fields, and no information on teaching quality or other unrecorded influences. G1/G2/G3 are strongly related to each other by construction of the course.

## 4. System architecture and workflow
```
 student-mat.csv ─► Spark read (sep=";", header, inferSchema)
        │
        ▼
 inspect: schema, count, sample, summary ─► null + duplicate checks
        │
        ▼
 clean (dropDuplicates, dropna on grades) ─► validate grades 0–20
        │
        ▼
 AverageMarks, Performance ─► groupBy/agg (studytime, failures, absences band, category)
        │                                   │
        ▼                                   ▼
  cleaned_students CSV              aggregated CSVs ─► small collect() ─► matplotlib charts
```

## 5. Apache Spark and the operations used
Apache Spark is a distributed data-processing engine; here it runs in local mode. A **SparkSession** is the entry point. A **DataFrame** is a distributed table with a schema. **Transformations** (`select`, `filter`, `withColumn`, `groupBy`, `agg`, `orderBy`, `dropDuplicates`) are lazy and only describe the computation; **actions** (`count`, `show`, `collect`, `write`) trigger execution. Operations used: `spark.read.csv`, `printSchema`, `summary`, `F.when/otherwise`, `F.avg`, `F.count`, `F.round`, `cache`, `coalesce`, and `write.csv`. The full data is never collected to the driver; only small aggregated tables are collected for charts.

## 6. Cleaning and transformation methodology
| Step | Handling |
|---|---|
| Nulls | Counted per column; rows with a null grade are dropped (grades are required for every analysis). |
| Duplicates | Exact duplicate rows are counted and removed. |
| Grade validation | Rows with G1/G2/G3 outside 0–20 are removed before categorising. |
| `AverageMarks` | `(G1+G2+G3)/3`, rounded to 2 dp. A descriptive average of recorded grades, **not a predicted mark**. |
| `Performance` | From G3 only: Low 0–9, Medium 10–13, High 14–20. |
| Absences | Banded 0, 1–5, 6–10, 11+ for readable grouping. |
| Privacy | The top-10 table shows only non-identifying columns; the dataset has no names. |

## 7. Analysis questions
1. How many students fall in each performance category, and what is the overall mean G3?
2. How do the mean G3 and group size vary by `studytime`?
3. How do they vary by `failures` and by absence band?
4. Which records have the highest G3?

Group differences are **associations in this sample**, not evidence of cause and effect. Small groups give unstable averages, so always read the `students` column next to `avg_G3`.

## 8. Results

### 8.1 Real-data results (to be completed by the student)
Run `python src/student_analysis.py --input data/student-mat.csv --output outputs`, then copy your genuine values:

| Item | Your value |
|---|---|
| Raw rows / cleaned rows | |
| Duplicates / null grades found | |
| Low / Medium / High counts | |
| Overall average G3 | |
| Avg G3 by studytime 1–4 | |
| Avg G3 by failures 0–3 | |
| Avg G3 by absence band | |

Insert your own charts from `outputs/chart_*.png` and write an interpretation for each, using wording such as "students who reported more study time had a higher average G3 in this sample".

### 8.2 SYNTHETIC demonstration only (random data, not real)
Run on `sample_data/synthetic_student_mat.csv` (395 randomly generated rows) purely to show the pipeline works. The generator deliberately builds in mild patterns, so these numbers say nothing about real students.

| Item | Synthetic run |
|---|---|
| Raw / cleaned rows | 395 / 395 |
| Duplicates / nulls / out-of-range grades | 0 / 0 / 0 |
| Low / Medium / High | 134 / 159 / 102 |
| Overall average G3 | 10.99 |

Charts and the full log: `reports/sample_output/` (log file and `chart_*.png`, all labelled synthetic).

## 9. Testing and execution evidence
- `python -m pytest tests -q` → **8 passed** (run in the build sandbox: Python 3.12, Java 21, PySpark 4.3.0). Tests cover category boundaries (0, 9, 10, 13, 14, 20), grade validation, `AverageMarks`, duplicate/null cleaning, group statistics, absence bands, category ordering and the top-N limit.
- The pipeline ran end to end on the synthetic file and produced all CSV folders and four PNG charts; a missing input file produces a clear error and exit code 1.
- **Not done:** no run on the real UCI file. Add your own screenshots here after running locally.

## 10. Limitations, ethics and conclusion
Limitations: descriptive only; single subject and two schools; small groups; no control for confounders. Ethics: student data is sensitive, so identifying details should not be displayed, results should not be used to label or rank individual students, and associations must not be presented as causes. Conclusion: the project demonstrates a complete, reproducible Spark DataFrame workflow for loading, cleaning, deriving columns, aggregating and exporting educational data; the substantive findings must come from your run on the real dataset.

## References
1. P. Cortez and A. Silva, "Using Data Mining to Predict Secondary School Student Performance," in Proc. FUBUTEC 2008. Dataset: https://archive.ics.uci.edu/dataset/320/student+performance
2. Apache Spark documentation, https://spark.apache.org/docs/latest/
3. PySpark API reference, https://spark.apache.org/docs/latest/api/python/
