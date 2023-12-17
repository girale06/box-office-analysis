from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, format_number
from schema import json_schema

json_path = "../datasets/data.json"

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

# Format the total gross column
genres_df = genres_df.withColumn("total_gross", format_number(col("total_gross"), 2))

genres_df.show(truncate=False)
spark.stop()
