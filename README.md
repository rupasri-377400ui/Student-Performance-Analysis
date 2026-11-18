# Student Performance Analysis Using Apache Spark

A beginner-friendly **descriptive analytics** mini-project built only with **Apache Spark (PySpark)**. It cleans the UCI Student Performance (Mathematics) data, derives `AverageMarks` and a `Performance` category, runs aggregations, and writes CSV results and charts.

It does **not** predict marks and does **not** claim any factor causes better or worse results.

## Prerequisites
- Python 3.9+ 
- Java 17 or 21 (JDK) installed, `java -version` must work (Spark needs it)
- Windows only: if Spark complains about `winutils`/`HADOOP_HOME`, see Troubleshooting

## Install
```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS / Linux:
source .venv/bin/activate
pip install -r requirements.txt
```

## Get the data
Download `student-mat.csv` from https://archive.ics.uci.edu/dataset/320/student+performance and put it in `data/` (see `data/README.md`).

## Run
```bash
python src/student_analysis.py --input data/student-mat.csv --output outputs
```
Options: `--input` (CSV path), `--output` (folder, created automatically), `--no-charts`.

Smoke test with the **synthetic** file (not real data):
```bash
python src/student_analysis.py --input sample_data/synthetic_student_mat.csv --output outputs/synthetic_demo
```

## Tests
```bash
python -m pytest tests -q
```

## Outputs (in `outputs/`)
Each result is a folder containing a Spark `part-*.csv` file (that is how Spark writes CSV):
`cleaned_students`, `performance_category_counts`, `avg_g3_by_studytime`, `avg_g3_by_failures`, `avg_g3_by_absences_band`, `top10_by_g3`, plus PNG charts `chart_*.png`.

## Project layout
```
src/transformations.py   reusable Spark transformations
src/student_analysis.py  main program (CLI)
tests/                   pytest tests
reports/project_report.md  mini-project report
reports/sample_output/   SYNTHETIC demo log + charts (not real results)
sample_data/             synthetic CSV + generator
```

## Troubleshooting
- **`JAVA_HOME is not set` / `java not found`**: install a JDK (17 or 21) and set `JAVA_HOME`.
- **Input file not found**: place `student-mat.csv` in `data/` or pass `--input`.
- **Windows `winutils.exe` warning/error**: set `HADOOP_HOME` to a folder containing `bin\winutils.exe`; warnings about it are often harmless for local CSV reading, but writing may fail without it.
- **Python/pandas warning from PySpark**: harmless for this project.
- Slow first start is normal (Spark launches a JVM).

## Status of verification
Unit tests and the pipeline were run in a Linux sandbox with Python 3.12, Java 21 and PySpark 4.3.0, using the **synthetic** file only. The real UCI file could not be downloaded there, so the pipeline has **not** been run on real data and no real-data results are included. You must run it yourself and capture your own screenshots.


## Localhost web application (manual entry)
This project also includes a Flask web interface. It accepts student details manually,
uses the existing PySpark transformations to calculate the average and performance
category, and displays the entered records and session-level distribution in a browser.

Run from the project root in PowerShell:
```powershell
pip install -r requirements.txt
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;" + $env:PATH
python app.py
```
Open `http://127.0.0.1:5000` in your browser. Enter student details and click
"Analyze student". Keep the terminal open while using the app. This demo stores
submissions in memory only; restarting the server clears them. The performance
category is based on the entered G3 grade; it is descriptive, not a prediction.
