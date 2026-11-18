"""Student Performance Analysis Using Apache Spark (PySpark).

Descriptive analytics only: no prediction, no causal claims.

Usage:
    python src/student_analysis.py --input data/student-mat.csv --output outputs
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import transformations as T  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Student performance analysis with PySpark")
    p.add_argument("--input", default="data/student-mat.csv",
                   help="Path to semicolon-separated student CSV")
    p.add_argument("--output", default="outputs", help="Output directory")
    p.add_argument("--no-charts", action="store_true", help="Skip PNG charts")
    return p.parse_args(argv)


def write_csv(df, out_dir, name):
    """Write a DataFrame as CSV (with header) using Spark's writer."""
    (df.coalesce(1).write.mode("overwrite").option("header", True)
       .csv(os.path.join(out_dir, name)))


def make_charts(out_dir, cat_rows, study_rows, abs_rows, fail_rows):
    """Draw charts from small aggregated results only (never the full dataset)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def bar(rows, key, title, xlabel, fname, ycol="avg_G3", ylabel="Average G3"):
        plt.figure(figsize=(6, 4))
        plt.bar([str(r[key]) for r in rows], [r[ycol] for r in rows], color="#3b6ea5")
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, fname), dpi=120)
        plt.close()

    bar(cat_rows, "Performance", "Students per performance category",
        "Category (by G3)", "chart_performance_categories.png", "students", "Students")
    bar(study_rows, "studytime", "Average G3 by study time (descriptive)",
        "studytime code", "chart_studytime_avg_g3.png")
    bar(abs_rows, "absences_band", "Average G3 by absences band (descriptive)",
        "absences", "chart_absences_avg_g3.png")
    bar(fail_rows, "failures", "Average G3 by past failures (descriptive)",
        "failures", "chart_failures_avg_g3.png")


def main(argv=None):
    args = parse_args(argv)

    if not os.path.isfile(args.input):
        print(f"ERROR: input file not found: {args.input}\n"
              "Download student-mat.csv from "
              "https://archive.ics.uci.edu/dataset/320/student+performance and place it "
              "in data/ (see data/README.md), or pass --input <path>.\n"
              "To only try the app, use the synthetic file: "
              "--input sample_data/synthetic_student_mat.csv", file=sys.stderr)
        return 1

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("StudentPerformanceAnalysis")
             .master("local[*]").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    os.makedirs(args.output, exist_ok=True)

    try:
        df = T.load_students(spark, args.input)
        required = T.GRADE_COLUMNS + ["studytime", "failures", "absences"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"ERROR: missing expected columns {missing}. Is this the "
                  "semicolon-separated student-mat.csv?", file=sys.stderr)
            return 1

        print("\n=== Schema ===")
        df.printSchema()
        print(f"Row count (raw): {df.count()}")
        print("\n=== Sample (5 rows) ===")
        df.limit(5).show(truncate=False)
        print("\n=== Summary statistics (G1, G2, G3, absences) ===")
        df.select("G1", "G2", "G3", "absences").summary().show()

        print("=== Null counts per column ===")
        T.count_nulls(df).show(vertical=True)
        print(f"Exact duplicate rows: {T.count_duplicates(df)}")

        cleaned = T.clean_students(df)
        valid = T.validate_grades(cleaned)
        print(f"Rows with grades outside 0-20 removed: {cleaned.count() - valid.count()}")
        data = T.add_performance_category(T.add_average_marks(valid)).cache()
        print(f"Row count (cleaned): {data.count()}")

        cats = T.category_counts(data)
        print("\n=== Students per performance category ===")
        cats.show()
        print(f"Overall average G3: {T.overall_average_g3(data):.2f}")

        study = T.group_stats(data, "studytime")
        print("\n=== Average G3 by studytime ===")
        study.show()
        fails = T.group_stats(data, "failures")
        print("=== Average G3 by failures ===")
        fails.show()
        absb = T.absences_band_stats(data)
        print("=== Average G3 by absences band ===")
        absb.show()
        top = T.top_n_by_g3(data, 10)
        print("=== Top 10 by G3 (non-identifying columns only) ===")
        top.show()

        write_csv(data, args.output, "cleaned_students")
        write_csv(cats, args.output, "performance_category_counts")
        write_csv(study, args.output, "avg_g3_by_studytime")
        write_csv(fails, args.output, "avg_g3_by_failures")
        write_csv(absb, args.output, "avg_g3_by_absences_band")
        write_csv(top, args.output, "top10_by_g3")

        if not args.no_charts:
            make_charts(args.output, cats.collect(), study.collect(),
                        absb.collect(), fails.collect())
        print(f"\nDone. Results written to: {os.path.abspath(args.output)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
