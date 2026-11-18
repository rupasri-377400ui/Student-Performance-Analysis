import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import transformations as T  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def make(spark, rows):
    return spark.createDataFrame(
        rows, "G1 int, G2 int, G3 int, studytime int, failures int, absences int")


def test_category_boundaries(spark):
    df = make(spark, [(0, 0, 0, 1, 0, 0), (9, 9, 9, 1, 0, 0), (10, 10, 10, 1, 0, 0),
                      (13, 13, 13, 1, 0, 0), (14, 14, 14, 1, 0, 0), (20, 20, 20, 1, 0, 0)])
    got = {r["G3"]: r["Performance"] for r in T.add_performance_category(df).collect()}
    assert got == {0: "Low", 9: "Low", 10: "Medium", 13: "Medium", 14: "High", 20: "High"}


def test_validate_grades_drops_out_of_range(spark):
    df = make(spark, [(10, 10, 10, 1, 0, 0), (10, 10, 25, 1, 0, 0), (-1, 5, 5, 1, 0, 0)])
    assert T.validate_grades(df).count() == 1


def test_average_marks(spark):
    df = make(spark, [(10, 12, 14, 1, 0, 0)])
    assert T.add_average_marks(df).first()["AverageMarks"] == 12.0


def test_clean_removes_duplicates_and_null_grades(spark):
    df = make(spark, [(10, 10, 10, 1, 0, 0), (10, 10, 10, 1, 0, 0), (5, 5, None, 1, 0, 0)])
    assert T.count_duplicates(df) == 1
    assert T.clean_students(df).count() == 1


def test_group_stats_studytime(spark):
    df = make(spark, [(1, 1, 10, 1, 0, 0), (1, 1, 14, 1, 0, 0), (1, 1, 8, 2, 0, 0)])
    rows = {r["studytime"]: (r["avg_G3"], r["students"])
            for r in T.group_stats(df, "studytime").collect()}
    assert rows == {1: (12.0, 2), 2: (8.0, 1)}


def test_absences_bands_in_order(spark):
    df = make(spark, [(1, 1, 10, 1, 0, 0), (1, 1, 10, 1, 0, 3),
                      (1, 1, 10, 1, 0, 8), (1, 1, 10, 1, 0, 20)])
    bands = [r["absences_band"] for r in T.absences_band_stats(df).collect()]
    assert bands == ["0", "1-5", "6-10", "11+"]


def test_category_counts_order(spark):
    df = T.add_performance_category(make(spark, [(1, 1, 15, 1, 0, 0), (1, 1, 5, 1, 0, 0),
                                                 (1, 1, 11, 1, 0, 0), (1, 1, 6, 1, 0, 0)]))
    rows = [(r["Performance"], r["students"]) for r in T.category_counts(df).collect()]
    assert rows == [("Low", 2), ("Medium", 1), ("High", 1)]


def test_top_n_limits_rows(spark):
    df = T.add_average_marks(make(spark, [(1, 1, g, 1, 0, 0) for g in range(15)]))
    top = T.top_n_by_g3(df, 10).collect()
    assert len(top) == 10 and top[0]["G3"] == 14
