# Questions we are trying to answer:
# 1. which movie genre has the best selling movies?
# 2. which director has the best selling movies?
# 3. which year provided the greatest amount of chart-topping movies?
# 4. which month is the most profitable 

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, ceil

def load_movie_data(spark, movie_path):
    movie_df = spark.read.json(movie_path)
    return movie_df

def main():
    spark = SparkSession.builder.appName("MovieRatingRanges").getOrCreate()

    movie_path = "data/data.json"

    movie_df = load_movie_data(spark, movie_path)

    # Calculate the genres with highest earning movies

    spark.stop()

if __name__ == '__main__':
    main()