# Question number 2
# Which movie director has the best selling movies?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from schema import json_schema

json_path = "small_data.json"

spark = SparkSession.builder.appName("MovieRatingRanges").getOrCreate()

json_data = spark.read.option("multiline", "true").schema(json_schema).json(json_path)

json_data.printSchema()

# print("------------------------")
# json_data.show(truncate=False)
# print("------------------------")

movies_df = json_data.withColumn("characters", explode("characters"))

# movies_df.show(truncate=False)
# print("------------------------")

directors_df = movies_df.filter(col("characters.peopleType") == "Director")

# directors_df.show(truncate=False)
# print("------------------------")

# Cast "boxOffice" to double
directors_df_df = directors_df.withColumn("boxOffice", col("boxOffice").cast("double"))

directors_df = directors_df.groupBy("characters.personName") \
            .agg({"boxOffice": "sum"}) \
            .withColumnRenamed("sum(boxOffice)", "total_gross")

# directors_df.show(truncate=False)
# print("------------------------")

max_gross_director = directors_df.orderBy(col("total_gross").desc()).first()

print(f"Director with the best selling movies: {max_gross_director['personName']}")
spark.stop()