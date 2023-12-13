from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, format_number, count
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

# Load the JSON data
json_file_path = "..data/data.json"
df = spark.read.option("multiline", "true").json(json_file_path)
df = df.withColumn("boxOffice", col("boxOffice").cast(DoubleType()))

# Exclude rows with null values
df = df.filter(col("year").isNotNull() & col("boxOffice").isNotNull())

# Extract month
df = df.withColumn("releaseMonth", month("first_release.date"))
result_df = df.groupBy("releaseMonth").agg(
    sum("boxOffice").alias("totalBoxOffice"),
    count("*").alias("numMoviesReleased")
).orderBy("releaseMonth")

result_df = result_df.withColumn("totalBoxOffice", format_number("totalBoxOffice", 2))
result_df.show(truncate=False)

spark.stop()
