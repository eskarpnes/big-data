from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import os
albums = os.path.join(os.getcwd(), "data/albums.csv")
artists = os.path.join(os.getcwd(), "data/artists.csv")

spark = SparkSession.builder.appName("Task10").getOrCreate()
cxt = spark.sparkContext
albums = cxt.textFile(albums)
artists = cxt.textFile(artists)
album_fields_with_type = [
    ('id', StringType()), ('artist_id', StringType()),
    ('album_title', StringType()), ('genre', StringType()),
    ('year_of_pub', StringType()), ('num_of_tracks', IntegerType()),
    ('num_of_sales', IntegerType()), ('rolling_stone_critic', DoubleType()),
    ('mtv_critic', DoubleType()), ('music_maniac_critic', DoubleType())
]

album_schema = StructType([
    StructField(_name, _type, True) for (_name, _type) in album_fields_with_type
])
df_album = spark.read.csv(albums, header=False, schema=album_schema)

artist_fields_with_type = [
    ('id', StringType()), ('real_name', StringType()),
    ('art_name', StringType()), ('role', StringType()),
    ('year_of_birth', StringType()), ('country', StringType()),
    ('city', StringType()), ('email', StringType()),
    ('zip_code', StringType())
]


artist_schema = StructType([
    StructField(_name, _type, True) for (_name, _type) in artist_fields_with_type
])
df_artist = spark.read.csv(artists, header=False, schema=artist_schema)


def get_distinct(df, col):
    return int(df.select(col).distinct().count())


def get_extrema(df, col, max=True):
    min_or_max = 'max' if max else 'min'
    return int(df.agg({col: min_or_max}).head()[0])


def task_10():
    queries = {
        'distinct_artists': get_distinct(df_artist, 'id'),
        'distinct_albums': get_distinct(df_album, 'id'),
        'distinct_genres': get_distinct(df_album, 'genre'),
        'distinct_countries': get_distinct(df_artist, 'country'),
        'min_year_pub': get_extrema(df_album, 'year_of_pub', max=False),
        'max_year_pub': get_extrema(df_album, 'year_of_pub'),
        'min_year_birth': get_extrema(df_artist, 'year_of_birth', max=False),
        'max_year_birth': get_extrema(df_artist, 'year_of_birth'),
    }
    _rows = [Row(metric=k, result=v) for k, v in queries.items()]
    df = spark.createDataFrame(cxt.parallelize(_rows))
    df.show()


task_10()
