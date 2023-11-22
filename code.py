#importing required libraries to get access to webpage html and parsing it and some pySpark libraries for data manipulation
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pyspark
from pyspark.sql.functions import col, max, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#Methods to get each columns and perform data cleaning
def get_movie_name(movie):

    try:
        movie_name = movie.find("h3").find("a").text

    except:
        movie_name = ""

    return movie_name


def get_genre(movie):

    try:
        genre = movie.find("span", attrs = {"class" : 'genre'}).text.strip()

    except:
        genre = ""

    return genre


def get_abstract(movie):

    try:
        abstract = movie.find("p", attrs = {"class" : ''}).text.strip()

    except:
        abstract = ""

    return abstract


def get_director(movie):

    try:
        director = movie.find_all("p", attrs = {"class" : 'text-muted text-small'})[1].find("a").text

    except:
        director = ""

    return director


def get_hero(movie):

    try:
        hero = movie.find_all("p", attrs = {"class" : 'text-muted text-small'})[1].find("a").find_next("a").text

    except:
        hero = ""

    return hero


def get_imdb_rating(movie):

    try:
        imdb_rating = float(movie.find("span", attrs = {"class" : 'ipl-rating-star__rating'}).text)

    except:
        imdb_rating = 0.0

    return imdb_rating


def get_votes(movie):

    try:
        votes = int(movie.find("span", attrs = {"name" : 'nv'}).text.replace(',',''))

    except:
        votes = 0

    return votes


def get_runtime(movie):

    try:
        runtime = int(movie.find("span", attrs = {"class" : 'runtime'}).text.split(" ")[0])

    except:
        runtime = 0

    return runtime


def get_language(details):

    try:
        language = details[0].split(":")[1].strip()

    except:
        language = ""

    return language


def get_budget(details):

    try:
        budget = float(details[1].split(":")[1].strip()[:-2])

    except:
        budget = 0.0

    return budget


def get_collections(details):

    try:
        collections = float(details[2].split(":")[1].strip()[:-2])

    except:
        collections = 0.0

    return collections


def get_verdict(details):

    try:
        verdict = details[4].split(":")[1].strip().strip('()') if details[4].split(":")[0].strip().strip('()') == "Verdict" else details[5].split(":")[1].strip().strip('()')

        if verdict == "Pending":verdict = "Still Running" 

    except:
        verdict = "Not yet Released"

    return verdict
    
    
#main method    
if __name__ == '__main__':

    #URL from IMDB page containing list of top 100 indian movies by collection which gets updated on daily basis
    URL = "https://www.imdb.com/list/ls562915122/"
    
    #for header verification in https link
    HEADERS = ({'User-Agent' : 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)', 'Accept-Language' : 'en-US, en;q = 0.5'})
    
    #sending requests for header verification
    webpage = requests.get(URL, headers = HEADERS)
 
    #using beautiful soup to parse and play with html file
    soup = BeautifulSoup(webpage.content, 'html.parser')
    
    #extracting all movie details into movies
    movies = soup.find_all("div", attrs = {"class" : 'lister-item mode-detail'})
 
    #looping thru each movie and storing details in dict
    d = {"movie_name" : [], "genre" : [], "abstract" : [], "director" : [], "hero" : [], "imdb_rating(out of 10)" : [],
         "votes" : [], "runtime(in mins)" : [], "language" : [], "budget(in Cr)" : [], "collections(in Cr)" : [], "verdict" : [] }
    i = 0
    for _ in movies:

        d["movie_name"].append(get_movie_name(movies[i]))
        d["genre"].append(get_genre(movies[i]))
        d["abstract"].append(get_abstract(movies[i]))
        d["director"].append(get_director(movies[i]))
        d["hero"].append(get_hero(movies[i]))
        d["imdb_rating(out of 10)"].append(get_imdb_rating(movies[i]))
        d["votes"].append(get_votes(movies[i]))
        d["runtime(in mins)"].append(get_runtime(movies[i]))

        adulterated_details = movies[i].find("div", attrs = {"class" : 'list-description'}).find("p").contents
        details = [item for item in adulterated_details if str(item) != '<br/>']
        
        d["language"].append(get_language(details))
        d["budget(in Cr)"].append(get_budget(details))
        d["collections(in Cr)"].append(get_collections(details))
        d["verdict"].append(get_verdict(details))

        i += 1

    movies_df = pd.DataFrame.from_dict(d)
    
display(movies_df)

#Creating PySpark DataFrame out of pandas DataFrame

#Enabling Apache Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

#Providing proper schema for spark dataFrame
movieSchema = pyspark.sql.types.StructType([
    StructField("movie_name", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("director", StringType(), True),
    StructField("hero", StringType(), True),
    StructField("imdb_rating(out of 10)", FloatType(), True),
    StructField("votes", IntegerType(), True),
    StructField("runtime(in mins)", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("budget(in Cr)", FloatType(), True),
    StructField("collections(in Cr)", FloatType(), True),
    StructField("verdict", StringType(), True)
])

spark_df = spark.createDataFrame(movies_df, schema = movieSchema)
spark_df.printSchema()

#Different data analytics using pyspark dataFrame

#highest grossing by language
highest_collection_by_language = spark_df.groupBy(col("language").alias("regional_language")).agg(max("collections(in Cr)").alias("collections")).orderBy(desc("collections"))
ans_df = spark_df.join(highest_collection_by_language, spark_df["collections(in Cr)"] == highest_collection_by_language["collections"], "inner").select("movie_name", "language", "collections")

display(ans_df)

#highest profit for an movie by percentage with budget greater than 50 Cr
ans_df = spark_df.filter(spark_df["budget(in Cr)"] > 50).withColumn("profit_percent", ((spark_df["collections(in Cr)"] - spark_df["budget(in Cr)"]) / spark_df["budget(in Cr)"]) * 100).select("movie_name", "language", "budget(in Cr)", "profit_percent").orderBy(desc("profit_percent"))

display(ans_df)


#low rated movies(rating <= 4) with collection more than 100Cr
ans_df = spark_df.filter((col("imdb_rating(out of 10)")  <= 4) & (col("collections(in Cr)") > 100)).select("movie_name")

display(ans_df)

#Top 10 highest imdb rated movies
ans_df = spark_df.orderBy(desc("imdb_rating(out of 10)")).limit(10).select("movie_name", "imdb_rating(out of 10)", "director", "hero", "collections(in Cr)")

display(ans_df)