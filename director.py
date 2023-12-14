# Question number 2
# Which movie director has the best selling movies?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from schema import json_schema

json_path = "data.json"

spark = SparkSession.builder.appName("Best selling director").getOrCreate()

# import json data with the predefined json schema
json_data = spark.read.option("multiline", "true").schema(json_schema).json(json_path)

# Explode the "characters" column
movies_df = json_data.withColumn("characters", explode("characters"))

# Filter the "characters" column to only include directors
directors_df = movies_df.filter(col("characters.peopleType") == "Director")

# Cast "boxOffice" to double
directors_df_df = directors_df.withColumn("boxOffice", col("boxOffice").cast("double"))

directors_df = directors_df.groupBy("characters.personName") \
            .agg({"boxOffice": "sum"}) \
            .withColumnRenamed("sum(boxOffice)", "total_gross")

# Get the director with the highest total gross
max_gross_director = directors_df.orderBy(col("total_gross").desc()).first()

print(f"Director with the best selling movies: {max_gross_director['personName']}")
spark.stop()