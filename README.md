# Box Office Analysis

**1. Description of the problem**  
We want to analyze the Box Office trying to find which movie genre and which director has the best-selling movies. According to it, we intend to check which year provided the greatest amount of chart-topping productions and which month is the most profitable time to release a movie.

**2. Description of the need for Big Data processing**  
Each movie has many features: directors, actors, film genre, release date, budget, earned awards. Which of them gives a movie the best chance to become a box office hit?  To answer this question, we need to analyze a huge amount of movies. This is where we will need Big  Data processing.

**3. Description of the data**  
Our data comes from the https://thetvdb.com website. This is the most accurate collection of data for TV and film. Their information is gathered by the contributors and fans, who are rewarded if the data is correct. There is metadata available for 147,000+ TV Series and 337,000+ movies, therefore TheTVDB has thorough and precise databases.
We will be focusing on movies in our project. The database contains numerous useful facts such as the title, cast & crew, characters, genres, date of release in particular countries, production budget, global box office, awards and much more, however we must carry out data cleansing work also, because the database contains a bunch of useless data for us for example links to artworks, trailers and titles in different languages.
We acquired the data using their API. After receiving an API key and making a 10EUR contribution, now we can use it freely. Since our project will mainly be completed in Python, TheTVDB’s python library (tvdb_v4_official) comes in handy. The metadata provider recommends making a local copy of the database, which we did in a json format. Everything we need to use we can find in their GitHub repository: https://github.com/thetvdb/v4-api. 
