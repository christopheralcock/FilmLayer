## How to run:

### Set up

[Install Python](https://realpython.com/installing-python/) 

[Install Scala](https://www.tutorialspoint.com/scala/scala_environment_setup.htm)

[Install Spark](https://spark.apache.org/downloads.html)

[Install Pyspark](https://towardsdatascience.com/how-to-get-started-with-pyspark-1adc142456ec)

[Install spark-xml](https://github.com/databricks/spark-xml)

[Install Postgres](https://www.postgresql.org/docs/9.3/tutorial-install.html)

[Download Postgres Spark driver jar](https://jdbc.postgresql.org/download.html)

[Download movie data](https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv)

[Download and extract wiki data](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz)

Set up Postgres database, entering the following in the [Postgres Terminal](http://postgresguide.com/utilities/psql.html):
```
create database truefilm;

create table film (
  title varchar,
  budget integer,
  release_date date,
  revenue integer,
  popularity decimal,
  budget_revenue_ration decimal,
  production_companies varchar,
  url varchar,
  abstract varchar
);
```

Run main.py with the following positional arguments, in the following order:

`jar_location` - the location of the Postgres spark JDBC jar that you have downloaded

`movie_db_location` - the location of the movie database CSV that you have downloaded

`wiki_location` - the location of the wiki data xml that you have downloaded

`joined_data_location` - the location at which you would like to store the full result of the join between the movie data and wiki data in order to do Spark queries on more than 1000 items which will be in the Postgres database 

`top_thousand_csv_location` - the location at which you would like to store the csv of the top one thousand results

`jdbc_url` = the url of your postgres database for spark to connect to it

### Querying

My solution obviously provides a Postgres database as clearly requested.  This can be queried with any number of tools, through the [Postgres Terminal](http://postgresguide.com/utilities/psql.html), or by building a small web interface.

I also provide a parquet dataset of the entire movie data set matched with wikipedia data, as the instructions imply that this is wanted, by asking for the data join before the top 1000 filter.  This can be queried with a Spark session, perhaps in a [Jupyter notebook](https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes) or perhaps by doctoring my python script.

I also took the step of outputting a csv of the same data that ended up in the postgres database. At a thousand items and with minimal barrier, opening this in Excel or similar will be the most user-friendly way for most people to access and query the data.

## Tool choice:

### Spark:

I chose Apache Spark to process the data because if we want to productionise and speed up the processing, Spark allows us to use a cluster and fine tune performance.

### Python:

I chose python as of the two main languages with Spark APIs it is the one with which I am more familiar

### Algorithmic choices:

As you will see from the annotations on my code, the biggest algorithmic choice I made was to filter the wiki data set early on in the process with a set of heuristics derived from interrogating the data.

The wiki dataset is massive, so by filtering it down to pages likely to be films we both avoid false matches against non film-pages, improving quality, and save processing effort by not joining on the full 6GB dataset.

I could have taken this approach further and only joined the top 1000 best budget:revenue ratio films in the movie data against the filtered wiki data set, but I wanted to provide a Parquet dataset of all of the Movie database matched against wiki data in order to allow more in-depth querying options to the end user.

I should also note that, were I doing this in production, I would probably simply not use this wiki data at all.  The "abstract" field is corrupted, completely inconsistent, and useless and providing a link to a wikipedia page for users to click through can be achieved in many simpler and more reliable, robust and future-proof ways, eg [https://duckduckgo.com/?q=%5C [film name] film wiki](https://duckduckgo.com/?q=%5C+Total+Recall+film+wiki)

##Data quality:

In production I would devise a lightweight testing strategy, demonstrating all of my Spark api usage with the edge cases it is deigned to handle

I would work to provide manual intervention tools at whatever stage of the data flow is necessary in order to allow the end user to ignore incorrect data that is coming from the sources, eg films claiming to have a budget of zero when this is literally impossible

For my results I have checked that the output of my process matches with commonly-cited examples of great budget:revenue ratios like Halloween and Mad Max.  Where films that don't in fact have particuarly good ratios in real life, but do so in my end data, that is because of incorrect input data, which I would provide stategies to mitigate.

## "Reproducible and automated"

Hopefully the fact that this is a python script that requires zero manual inspection of the data meets the definition of "Reproducible and automated".

In production I would ascertain how frequently the data needs to be updated and perhaps choose an automated cloud solution which could run at a set interval to keep the process 100% automated.

I would have included an HTTP client in my script, preventing the need for you to download the files manually, but unfortunately and perhaps recently, the movie data set's URL does not allow direct download and I didn't think you'd want me to make a Selenium script to get around that or whatever.
