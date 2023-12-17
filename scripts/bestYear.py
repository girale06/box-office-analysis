from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, format_number, count
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

# Load the JSON data
json_file_path = "../data/combined_file.json"
df = spark.read.option("multiline", "true").json(json_file_path)
df = df.withColumn("boxOffice", col("boxOffice").cast(DoubleType()))

# Exclude rows with null values
df = df.filter(col("year").isNotNull() & col("boxOffice").isNotNull())

# Group by year and calculate the total box office revenue and the number of movies for each year
result_df = df.groupBy("year").agg(
    sum("boxOffice").alias("totalBoxOffice"),
    count("*").alias("numMoviesReleased")
).orderBy("year")

result_df = result_df.withColumn("totalBoxOffice", format_number("totalBoxOffice", 2))

max_gross_year = result_df.orderBy(col("totalBoxOffice").desc()).first()

print(f"Best selling year: {max_gross_year['year']}")
spark.stop()
