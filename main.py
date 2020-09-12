from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f
import sys


def main(command_line_args):

    jar_location = command_line_args[0]
    movie_db_location = command_line_args[1]
    wiki_location = command_line_args[2]
    joined_data_location = command_line_args[3]
    top_thousand_csv_location = command_line_args[4]
    jdbc_url = command_line_args[5]

    # Create spark session - jar location needs to be correct
    spark = SparkSession \
        .builder \
        .appName("TrueFilm") \
        .config("spark.jars", jar_location) \
        .getOrCreate()

    # Read in the movie db
    movie_db = spark.read\
        .option("header", "true")\
        .option("escape", "\"")\
        .format("csv")\
        .load(movie_db_location)

    # Calculate and store the budget to revenue ratio
    movie_db = movie_db.withColumn(
        "budget_revenue_ratio",
        (f.col("budget")/f.col("revenue")))

    # Read in the wiki data
    wiki = spark.read\
        .format("xml")\
        .option("rowTag", "doc")\
        .load(wiki_location)

    """
    Use broad heuristic derived from data interrogation to cut wiki data down to just films. Saves time and also saves 
    false matches against non film pages
    """
    wiki = wiki.filter(
        "title like '%film)%' OR links like '%Cast%' OR links like '%Plot%' OR links like '%Reception%'")

    # Create a year column for the sake of disambiguating films with same name as other films
    wiki = wiki.withColumn('year',
                           f.when(
                               f.regexp_extract('title', r'\d{4} film\)$', 0) != '',
                               f.regexp_extract('title', r'\d{4} film\)$', 0)[1:4]))

    """
    create a title column that more reliably matches against the movie db's title column, by dealing with "Wikipedia: " 
    in title and "film)" suffixes
    """
    wiki = wiki.withColumnRenamed("title", "wiki_page_title").withColumn(
        "match_title", f.trim(f.regexp_replace('wiki_page_title', r'Wikipedia\:|\([0123456789 ]{0,5}film\)', '')))

    """
    I wanted to do a conditional join of sorts here that allows me to match on title and year where possible, and 
    otherwise fallback to just title
    I couldn't quite get it, so instead created a multistage process to get the results I wanted, prioritising data 
    accuracy and usefulness over performance, which I think is appropriate given the context and the speed at which
    film revenues are announced compared to, say, visitors on a website
    """

    # Split wiki set into films with years and films without
    wiki_just_years = wiki.filter("year != ''")
    wiki_not_years = wiki.filter("year IS NULL")

    # First pass, where we have years, join on title and year
    db_years = movie_db.join(
        wiki_just_years,
        [
            movie_db['title'] == wiki_just_years['match_title'],
            f.trim(movie_db["release_date"])[1:4] == wiki_just_years['year']
        ],
        'inner')

    # Remove movie_db records that have already been matched with year
    db_not_years = movie_db.join(db_years, ['id'], 'leftanti')

    # Second pass, where we have no wiki year information to use, just match on Title
    db_not_years = db_not_years.join(
        wiki_not_years,
        db_not_years['title'] == wiki_not_years['match_title'],
        'left_outer')

    # Combine results from both passes
    movie_db = db_not_years.union(db_years).drop('match_title')

    # Save joined dataset for future querying
    movie_db.write.save(joined_data_location)

    # Rank by budget:revenue ratio
    budget_order = Window.orderBy(movie_db["budget_revenue_ratio"].asc())

    """
    considered setting a budget floor much higher to remove some clearly incorrect information, but who's to say that 
    when it says "1" instead of "1,000,000" that the same thing hasn't happened in the revenue?
    """
    top_thousand = movie_db.filter("budget_revenue_ratio IS NOT NULL AND budget > 0")\
        .select('*', f.rank().over(budget_order).alias('rank'))\
        .filter(f.col('rank') <= 1000)

    """
    Drop superfluous columns. Keeping release date, not year, because throwing away that data is a bad idea in my 
    opinion
    """
    top_thousand = top_thousand.select(
        'title',
        'budget',
        'release_date',
        'revenue',
        'popularity',
        'budget_revenue_ratio',
        'production_companies',
        'url',
        'abstract')

    # Save top thousand results as csv for ultra accessible lay-person Excel querying for very little effort
    top_thousand.repartition(1).write.csv(path=top_thousand_csv_location, header=True)

    # Write top thousand results to Postgres
    top_thousand.write.jdbc(
        url=jdbc_url,
        table='film',
        mode='overwrite')


if __name__ == "__main__":
    main(sys.argv)
