# Box Office Analysis

## 1. Description of the problem  
We want to analyze the Box Office trying to find which movie genre and which director has the best-selling movies. According to it, we intend to check which year provided the greatest amount of chart-topping productions and which month is the most profitable time to release a movie.

## 2. Description of the need for Big Data processing  
Each movie has many features: directors, actors, film genre, release date, budget, earned awards. Which of them gives a movie the best chance to become a box office hit?  To answer this question, we need to analyze a huge amount of movies. This is where we will need Big  Data processing.

## 3. Description of the data  
Our data comes from the https://thetvdb.com website. This is the most accurate collection of data for TV and film. Their information is gathered by the contributors and fans, who are rewarded if the data is correct. There is metadata available for 147,000+ TV Series and 337,000+ movies, therefore TheTVDB has thorough and precise databases.
We will be focusing on movies in our project. The database contains numerous useful facts such as the title, cast & crew, characters, genres, date of release in particular countries, production budget, global box office, awards and much more, however we must carry out data cleansing work also, because the database contains a bunch of useless data for us for example links to artworks, trailers and titles in different languages.
We acquired the data using their API. After receiving an API key and making a 10EUR contribution, now we can use it freely. Since our project will mainly be completed in Python, TheTVDB’s python library (tvdb_v4_official) comes in handy. The metadata provider recommends making a local copy of the database, which we did in a json format. Everything we need to use we can find in their GitHub repository: https://github.com/thetvdb/v4-api. 

## 4. Description of the solution.
### Software
The following scripts have been developed in Python and a brief description of them is provided. By clicking on its name you can see the code, located in the scripts folder. (Disclaimer: The scripts are made for data.json! If you want to run them for the full dataset you have to delete the delete the small data and rename the full dataset to data.json or change the path inside the code)
- [**bestMonth.py**](/scripts/bestMonth.py): A graph is obtained with all months, how many movies are released that month and which month has the best revenue. This will show us which month is most profitable to release the movie in.
- [**bestYear.py**](/scripts/bestYear.py): We obtain the most profitable year in all times. Using this we can make some predictions about the upcoming years.
- [**director.py**](/scripts/director.py): Shows which movie director has the most revenue. This way we can see who is the most successful director.
- [**genre.py**](/scripts/genre.py): The program shows which are the most popular genres, through the number of revenue made on these movies to make it easier for directors to choose a genre for their next movie that can be successful among the public.
- [**schema.py**](/scripts/schema.py): Here we use schema for the JSON file.It enables applications to validate data, ensuring it meets the defined criteria. With JSON Schema, we can make our JSON more readable, enforce data validation, and improve interoperability across different programming languages.
- in **Folder Results** you can find the test ran data.json which has small number of movies.
### Tools and work environment
To develop our study, we have used the following tools and technologies:
1. **Google Cloud**, for the execution of the scripts, storage and data management.
2. **GitHub**, for file management and version control.
3. **Python**, as the programming language of the scripts presented as a solution.
4. **PySpark**, as an interface to Apache Spark in Python, to perform operations on datasets and make use of parallel functional programming.
5. **Excel**, for the graphic representation of the results obtained in the performance analysis.
6. **GitHub Pages**, For the WebPage.

### Setting Up
Detailed steps for reproducing our study.
1. **Java and Python installation**<br />
```
$sudo apt install default-jre pip
```
2. **Spark installation**
```
$pip install pyspark
```
3. **Environment configuration** <br />
/usr/local/spark/bin is added to the PATH in the ~/.profile file. After updating the PATH in the current session.
```
$ echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
$ source ~/.profile
```
4. **File download**
The folders scripts and data can be created manually and copy the datasets and scripts to the corresponding ones, although it is also possible to download this repository and unzip it, by Download ZIP. 
To download the full dataset go to this site https://console.cloud.google.com/storage/browser/cloudandbigdataproject-408414;tab=objects?forceOnBucketsSortingFiltering=false&authuser=1&project=cloudandbigdataproject-408414&prefix=&forceOnObjectsSortingFiltering=false and write to sdobrev@ucm.es with your mail to get access.

5. **On Cloud**
Put everything in a Cloud Bucket and create a cluster by running the following code
```
gcloud dataproc clusters create example-cluster --region europe-west6 --master-boot-disk-size 50GB --worker-boot-disk-size 50GB --enable-component-gateway
```
Than submit the PySpack job(your_script.py is the script you want to run!)
```
gcloud dataproc jobs submit pyspark \
  --cluster=example-cluster \
  --region=europe-west6 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  gs://cloudandbigdataproject-408414/your_script.py

```
The result will be shown.
CPU Usage for the script for Best Year:
![Alt text](</pictures/CPU utilizationbestYear.png>)
CPU Usage for the script for Best Month:
![Alt text](</pictures/CPU utilizationbestMonth.png>)
CPU Usage for the script for Best Directors:
![Alt text](</pictures/CPU utilizationdirectors.png>)
CPU Usage for the script for Best Genres:
![Alt text](</pictures/CPU utilizationGenres.png>)
