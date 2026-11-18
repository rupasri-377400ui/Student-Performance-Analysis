from flask import Flask, render_template, request, redirect, url_for, flash
from pyspark.sql import SparkSession, functions as F
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "src"))
import transformations as T

app = Flask(__name__)
app.secret_key = "local-development-key-change-before-deployment"

# Demo records are kept in memory while this Flask process is running.
students = []

spark = (SparkSession.builder
         .appName("StudentPerformanceWebApp")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def analyze_records():
    """Run the existing project transformations and build dashboard metrics."""
    if not students:
        return None

    rows = [{
        "school": "Manual",
        "student_name": s["name"],
        "sex": s["sex"],
        "age": s["age"],
        "studytime": s["studytime"],
        "failures": s["failures"],
        "absences": s["absences"],
        "G1": s["G1"],
        "G2": s["G2"],
        "G3": s["G3"],
    } for s in students]

    df = spark.createDataFrame(rows)
    cleaned = T.clean_students(df)
    valid = T.validate_grades(cleaned)
    analyzed = T.add_performance_category(T.add_average_marks(valid)).cache()

    counts_raw = {
        row["Performance"]: row["students"]
        for row in T.category_counts(analyzed).collect()
    }
    counts = {key: counts_raw.get(key, 0) for key in ["Low", "Medium", "High"]}

    exam_averages_row = analyzed.agg(
        F.avg("G1").alias("first"),
        F.avg("G2").alias("second"),
        F.avg("G3").alias("final"),
    ).first()

    metrics = {
        "total": analyzed.count(),
        "average": round(float(analyzed.agg(
            F.avg("AverageMarks")
        ).first()[0] or 0), 2),
        "counts": counts,
        "exam_averages": [
            {"label": "First exam", "value": round(float(exam_averages_row["first"] or 0), 2)},
            {"label": "Second exam", "value": round(float(exam_averages_row["second"] or 0), 2)},
            {"label": "Final exam", "value": round(float(exam_averages_row["final"] or 0), 2)},
        ],
        "overall_final": round(float(exam_averages_row["final"] or 0), 2),
    }
    analyzed.unpersist()
    return metrics


def render_home(results=None, comparison=None, status=200):
    dashboard = analyze_records()
    return render_template(
        "index.html",
        students=students,
        results=results,
        dashboard=dashboard,
        comparison=comparison,
    ), status


@app.route("/", methods=["GET"])
def home():
    return render_home()


@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        student = {
            "name": request.form["name"].strip(),
            "age": int(request.form["age"]),
            "sex": request.form["sex"],
            "studytime": int(request.form["studytime"]),
            "failures": int(request.form["failures"]),
            "absences": int(request.form["absences"]),
            "G1": int(request.form["G1"]),
            "G2": int(request.form["G2"]),
            "G3": int(request.form["G3"]),
        }

        if not student["name"]:
            raise ValueError("Please enter the student's name.")
        if student["sex"] not in ("F", "M"):
            raise ValueError("Please select a valid gender.")
        if not 15 <= student["age"] <= 25:
            raise ValueError("Age must be between 15 and 25.")
        if not 1 <= student["studytime"] <= 4:
            raise ValueError("Study time must be between 1 and 4.")
        if not 0 <= student["failures"] <= 3:
            raise ValueError("Past failed subjects must be between 0 and 3.")
        if student["absences"] < 0:
            raise ValueError("Absences cannot be negative.")
        for grade in ("G1", "G2", "G3"):
            if not 0 <= student[grade] <= 20:
                raise ValueError("Exam marks must be between 0 and 20.")

        students.append(student)

        dashboard = analyze_records()
        average = round((student["G1"] + student["G2"] + student["G3"]) / 3, 2)
        category = ("Low" if student["G3"] <= 9 else
                    "Medium" if student["G3"] <= 13 else "High")
        results = {
            "name": student["name"],
            "average": average,
            "category": category,
            "count": dashboard["total"],
            "overall": dashboard["overall_final"],
            "counts": dashboard["counts"],
        }
        return render_home(results)

    except (ValueError, KeyError) as exc:
        flash(str(exc), "error")
        return render_home(status=400)
    except Exception:
        app.logger.exception("Student analysis failed")
        flash("Analysis could not be completed. Please check the input and try again.", "error")
        return render_home(status=500)


@app.route("/compare", methods=["POST"])
def compare_students():
    """Compare two or more students selected from the current session."""
    try:
        selected_indices = request.form.getlist("student_indices")
        if len(selected_indices) < 2:
            flash("Please select at least two students to compare.", "error")
            return redirect(url_for("home"))

        selected = []
        for raw_index in selected_indices:
            index = int(raw_index)
            if index < 0 or index >= len(students):
                raise ValueError("Invalid student selection. Please try again.")
            selected.append(students[index])

        comparison = []
        for student in selected:
            average = round((student["G1"] + student["G2"] + student["G3"]) / 3, 2)
            category = ("Low" if student["G3"] <= 9 else
                        "Medium" if student["G3"] <= 13 else "High")
            comparison.append({
                "name": student["name"],
                "G1": student["G1"],
                "G2": student["G2"],
                "G3": student["G3"],
                "average": average,
                "category": category,
            })
        return render_home(comparison=comparison)
    except (ValueError, TypeError):
        flash("Please select valid student records.", "error")
        return redirect(url_for("home"))


@app.route("/clear", methods=["POST"])
def clear():
    students.clear()
    flash("All manually entered records have been cleared.", "success")
    return redirect(url_for("home"))


if __name__ == "__main__":
    try:
        app.run(host="127.0.0.1", port=5000, debug=False)
    finally:
        spark.stop()
