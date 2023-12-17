from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, format_number
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType


company_type_schema = StructType([
    StructField("companyTypeId", IntegerType(), True),
    StructField("companyTypeName", StringType(), True)
])

production_countries_schema = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("name", StringType(), True)
]))

inspiration_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("type_name", StringType(), True),
    StructField("url", StringType(), True)
])

spoken_languages_schema = ArrayType(StringType())

first_release_schema = StructType([
    StructField("country", StringType(), True),
    StructField("date", StringType(), True),
    StructField("detail", StringType(), True)
])

json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("lastUpdated", StringType(), True),
    StructField("year", StringType(), True),
    StructField("genres", ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("slug", StringType(), True)
        ])
    ), True),
    StructField("characters", ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("peopleId", IntegerType(), True),
            StructField("seriesId", IntegerType(), True),
            StructField("series", StringType(), True),
            StructField("movie", StringType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("episodeId", IntegerType(), True),
            StructField("type", IntegerType(), True),
            StructField("image", StringType(), True),
            StructField("sort", IntegerType(), True),
            StructField("isFeatured", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("nameTranslations", StringType(), True),
            StructField("overviewTranslations", StringType(), True),
            StructField("aliases", StringType(), True),
            StructField("peopleType", StringType(), True),
            StructField("personName", StringType(), True),
            StructField("tagOptions", StringType(), True),
            StructField("personImgURL", StringType(), True)
        ])
    ), True),
    StructField("budget", StringType(), True),
    StructField("boxOffice", StringType(), True),
    StructField("boxOfficeUS", StringType(), True),
    StructField("originalCountry", StringType(), True),
    StructField("originalLanguage", StringType(), True),
    StructField("audioLanguages", ArrayType(StringType()), True),
    StructField("subtitleLanguages", ArrayType(StringType()), True),
    StructField("studios", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("parentStudio", IntegerType(), True)
    ])), True),
    StructField("awards", StringType(), True),
    StructField("contentRatings", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("description", StringType(), True),
        StructField("contentType", StringType(), True),
        StructField("order", IntegerType(), True),
        StructField("fullname", StringType(), True)
    ])), True),
    StructField("companies", StructType([
        StructField("studio", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("slug", StringType(), True),
            StructField("nameTranslations", StringType(), True),
            StructField("overviewTranslations", StringType(), True),
            StructField("aliases", StringType(), True),
            StructField("country", StringType(), True),
            StructField("primaryCompanyType", IntegerType(), True),
            StructField("activeDate", StringType(), True),
            StructField("inactiveDate", StringType(), True),
            StructField("companyType", company_type_schema, True),
            StructField("parentCompany", StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("relation", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("typeName", StringType(), True)
                ]), True)
            ]), True),
            StructField("tagOptions", StringType(), True)

        ]), True)),
        StructField("network", ArrayType(StringType()), True),
        StructField("production", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("slug", StringType(), True),
            StructField("nameTranslations", StringType(), True),
            StructField("overviewTranslations", StringType(), True),
            StructField("aliases", StringType(), True),
            StructField("country", StringType(), True),
            StructField("primaryCompanyType", IntegerType(), True),
            StructField("activeDate", StringType(), True),
            StructField("inactiveDate", StringType(), True),
            StructField("companyType", company_type_schema, True),
            StructField("parentCompany", StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("relation", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("typeName", StringType(), True)
                ]), True)
            ]), True),
            StructField("tagOptions", StringType(), True)
        ]), True)),
        StructField("distributor", ArrayType(StringType()), True),
        StructField("special_effects", ArrayType(StringType()), True)
    ]), True),
    StructField("production_countries", production_countries_schema, True),
    StructField("inspirations", ArrayType(inspiration_schema), True),
    StructField("spoken_languages", spoken_languages_schema, True),
    StructField("first_release", first_release_schema, True)
])

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

directors_df = directors_df.orderBy(col("total_gross").desc())

# Format the total gross column
directors_df = directors_df.withColumn("total_gross", format_number(col("total_gross"), 2))

# Show all rows
directors_df.show(n=directors_df.count(), truncate=False)

print(f"Director with the best selling movies: {directors_df.first()['personName']}, with a total gross of {directors_df.first()['total_gross']}")

spark.stop()