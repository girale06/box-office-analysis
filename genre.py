# Question number 1
# Which movie genre hast the best selling movies in total?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from schema import json_schema

json_path = "data.json"

spark = SparkSession.builder.appName("Best selling genre").getOrCreate()

# import json data with the predefined json schema
json_data = spark.read.option("multiline", "true").schema(json_schema).json(json_path)

# Explode the "genres" column
movies_df = json_data.withColumn("genres", explode("genres"))

# Cast "boxOffice" to double
movies_df = movies_df.withColumn("boxOffice", col("boxOffice").cast("double"))

# Group by "genres" and sum "boxOffice"
genres_df = movies_df.groupBy("genres.name") \
            .agg({"boxOffice": "sum"}) \
            .withColumnRenamed("sum(boxOffice)", "total_gross")

# Get the genre with the highest total gross
max_gross_genre = genres_df.orderBy(col("total_gross").desc()).first()

print(f"Genre with the best selling movies: {max_gross_genre['name']}")