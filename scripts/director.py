from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, format_number
from schema import json_schema

json_path = "../datasets/data.json"

spark = SparkSession.builder.appName("Best selling director").getOrCreate()

# import json data with the predefined json schema
json_data = spark.read.option("multiline", "true").schema(json_schema).json(json_path)

# Explode the "characters" column
movies_df = json_data.withColumn("characters", explode("characters"))

# Filter the "characters" column to only include directors
directors_df = movies_df.filter(col("characters.peopleType") == "Director")

# Cast "boxOffice" to double
directors_df = directors_df.withColumn("boxOffice", col("boxOffice").cast("double"))

# Group by director and sum the box office
directors_df = directors_df.groupBy("characters.personName") \
            .agg({"boxOffice": "sum"}) \
            .withColumnRenamed("sum(boxOffice)", "total_gross")

# Format the total gross column
directors_df = directors_df.withColumn("total_gross", format_number(col("total_gross"), 2))

# Show all rows
directors_df.show(n=directors_df.count(), truncate=False)

spark.stop()