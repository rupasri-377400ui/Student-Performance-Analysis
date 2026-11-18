"""Reusable Spark DataFrame transformations for the Student Performance project.

All functions take and return Spark DataFrames (or small aggregated results),
so they can be unit-tested on tiny in-memory data.
"""
from pyspark.sql import DataFrame, functions as F

GRADE_COLUMNS = ["G1", "G2", "G3"]
GRADE_MIN, GRADE_MAX = 0, 20
CATEGORY_ORDER = ["Low", "Medium", "High"]


def load_students(spark, path: str) -> DataFrame:
    """Read the semicolon-separated CSV with header and inferred types."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("inferSchema", True)
            .csv(path))


def count_nulls(df: DataFrame) -> DataFrame:
    """One-row DataFrame with the number of nulls per column."""
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])


def count_duplicates(df: DataFrame) -> int:
    """Number of rows that are exact copies of another row."""
    return df.count() - df.dropDuplicates().count()


def clean_students(df: DataFrame) -> DataFrame:
    """Documented cleaning: drop exact duplicate rows and rows with a null grade."""
    return df.dropDuplicates().dropna(subset=GRADE_COLUMNS)


def validate_grades(df: DataFrame) -> DataFrame:
    """Keep only rows whose G1, G2, G3 are all within 0-20."""
    cond = F.lit(True)
    for c in GRADE_COLUMNS:
        cond = cond & F.col(c).between(GRADE_MIN, GRADE_MAX)
    return df.filter(cond)


def add_average_marks(df: DataFrame) -> DataFrame:
    """AverageMarks = mean(G1, G2, G3), rounded to 2 dp.

    This is a DESCRIPTIVE average of three recorded grades, not a prediction.
    """
    return df.withColumn("AverageMarks",
                         F.round((F.col("G1") + F.col("G2") + F.col("G3")) / 3, 2))


def add_performance_category(df: DataFrame) -> DataFrame:
    """Low = 0-9, Medium = 10-13, High = 14-20, based on final grade G3.

    Callers should run validate_grades() first; out-of-range grades become null.
    """
    return df.withColumn(
        "Performance",
        F.when(F.col("G3").between(0, 9), "Low")
         .when(F.col("G3").between(10, 13), "Medium")
         .when(F.col("G3").between(14, 20), "High")
         .otherwise(None))


def category_counts(df: DataFrame) -> DataFrame:
    """Students per performance category, in Low/Medium/High order."""
    order = F.when(F.col("Performance") == "Low", 0).when(
        F.col("Performance") == "Medium", 1).otherwise(2)
    return (df.groupBy("Performance").agg(F.count("*").alias("students"))
              .withColumn("_o", order).orderBy("_o").drop("_o"))


def overall_average_g3(df: DataFrame) -> float:
    return df.agg(F.avg("G3")).first()[0]


def group_stats(df: DataFrame, column: str) -> DataFrame:
    """Average G3 and student count per value of `column` (descriptive only)."""
    return (df.groupBy(column)
              .agg(F.round(F.avg("G3"), 2).alias("avg_G3"),
                   F.count("*").alias("students"))
              .orderBy(column))


def absences_band_stats(df: DataFrame) -> DataFrame:
    """Absences grouped into bands (0, 1-5, 6-10, 11+) with average G3."""
    banded = df.withColumn(
        "absences_band",
        F.when(F.col("absences") == 0, "0")
         .when(F.col("absences") <= 5, "1-5")
         .when(F.col("absences") <= 10, "6-10")
         .otherwise("11+"))
    order = (F.when(F.col("absences_band") == "0", 0)
              .when(F.col("absences_band") == "1-5", 1)
              .when(F.col("absences_band") == "6-10", 2)
              .otherwise(3))
    return (banded.groupBy("absences_band")
                  .agg(F.round(F.avg("G3"), 2).alias("avg_G3"),
                       F.count("*").alias("students"))
                  .withColumn("_o", order).orderBy("_o").drop("_o"))


def top_n_by_g3(df: DataFrame, n: int = 10) -> DataFrame:
    """Top-n rows by G3 using only a safe, non-identifying column subset."""
    safe = [c for c in ["school", "sex", "age", "studytime", "failures",
                        "absences", "G1", "G2", "G3", "AverageMarks", "Performance"]
            if c in df.columns]
    ranked = df.orderBy(F.col("G3").desc(), F.col("AverageMarks").desc(),
                        *[F.col(c) for c in safe])
    return ranked.select(safe).limit(n)
