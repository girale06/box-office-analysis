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