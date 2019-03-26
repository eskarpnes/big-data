# coding: utf-8
from pyspark import SparkContext
import os

albums = os.path.join(os.getcwd(), "data/albums.csv")
artists = os.path.join(os.getcwd(), "data/artists.csv")

sc = SparkContext("local", "Big D")

albums_raw = sc.textFile(albums)  # one list with all the data
# proper csv format (list of lists)
albums_rdd = albums_raw.map(lambda x: x.split(','))

artists_raw = sc.textFile(artists)
artists_rdd = artists_raw.map(lambda x: x.split(','))


def task_1():
    print albums_rdd.map(lambda x: x[3]).distinct().count()


task_1()


def task_2():
    print artists_rdd.sortBy(lambda x: x[4]).first()[4]


task_2()

'''
HELPER FUNCTIONS :-)
'''


def count_column_val(rdd, col):
    return rdd.map(lambda x: x[col]).countByValue()


def sort_key_val(data, key_is_num=False):
    def sort((k, v)): return (-v, k)
    if key_is_num:
        # make sure we sort by int value and not str
        # e.g "764">"6082"
        def sort((k, v)): return (-v, int(k))
    return [x for x in sorted(data.items(), key=sort)]


def rdd(data):
    return sc.parallelize(data)


def delimit(*args):
    parsed = []
    for arg in args:
        tmp = arg
        if type(arg) == int or type(arg) == float:
            tmp = str(arg)
        elif type(arg) != str:
            tmp = arg.encode('utf-8')
        parsed.append(tmp)
    return '\t'.join(parsed)


def save_as_tsv(data, filename):
    as_rdd = rdd(data)
    delimited = as_rdd.map(lambda x: delimit(*x))
    delimited.saveAsTextFile(os.path.join(os.getcwd(), 'results', filename))


'''
END HELPER FUNCS
'''


def task_3():
    COL_COUNTRY = 5
    sorted_by_country = sort_key_val(
        count_column_val(artists_rdd, COL_COUNTRY))
    save_as_tsv(sorted_by_country, 'result_3.tsv')


task_3()


def task_4():
    COL_ARTIST_ID = 1
    sorted_by_albums = sort_key_val(count_column_val(
        albums_rdd, COL_ARTIST_ID), key_is_num=True)
    save_as_tsv(sorted_by_albums, 'result_4.tsv')


task_4()


def task_5():
    COL_GENRE, COL_NUM_SALES = 3, 6
    def genre_sales(x): return (x[COL_GENRE], x[COL_NUM_SALES])
    def add(a, b): return int(a) + int(b)
    sales_per_genre = albums_rdd.map(
        genre_sales).reduceByKey(add).collectAsMap()
    sorted_sales = sort_key_val(sales_per_genre)
    save_as_tsv(sorted_sales, 'result_5.tsv')


task_5()


def task_6():
    ID = 0
    ROLLING_STONE, MTV, MUSIC_MANIAC = 7, 8, 9
    def get_critics(x): return (x[ID], (
        float(x[ROLLING_STONE]) + float(x[MTV]) + float(x[MUSIC_MANIAC]))/3)
    avg_critic = albums_rdd.map(get_critics).takeOrdered(10, lambda (_id, avg): -avg)
    save_as_tsv(avg_critic, 'result_6.tsv')
    # print avg_critic


task_6()


def task_7():
    ID = 0
    ARTIST_ID = 1
    ROLLING_STONE, MTV, MUSIC_MANIAC = 7, 8, 9
    def get_critics(x): return (x[ID], x[ARTIST_ID], (
        float(x[ROLLING_STONE]) + float(x[MTV]) + float(x[MUSIC_MANIAC]))/3)
    avg_critic = albums_rdd.map(get_critics).takeOrdered(10, lambda (_id, _art_id, avg): -avg)
    artist_country = artists_rdd.map(lambda x: (x[0], x[5])).collectAsMap()
    country_critics = [(x[0], artist_country[x[1]], x[2]) for x in avg_critic]
    # print(country_critics)
    save_as_tsv(country_critics, 'result_7.tsv')


task_7()


def task_8():
    ARTIST_ID = 1  # in album rdd
    MTV = 8
    best_mtv_albums = set(albums_rdd.filter(lambda x: x[MTV] == '5').map(
        lambda x: x[1]).collect())
    artists_name = [[artist] for artist in set(artists_rdd.filter(lambda x: x[0] in best_mtv_albums).map(
        lambda x: x[1]).distinct().collect())]
    save_as_tsv(sorted(artists_name), 'result_8.tsv')


task_8()


def task_9():
    artists_from_norway = set(artists_rdd.filter(lambda x: x[5] == 'Norway').map(
        lambda x: (x[0], x[1], x[2])).collect())
    artists_id = set([x[0] for x in artists_from_norway])
    albums_from_norway = albums_rdd.filter(lambda x: x[1] in artists_id).map(
        lambda x: (x[1], x[8])).groupByKey().mapValues(list).collect()

    artists_name = artists_rdd.filter(lambda x: x[0] in artists_id).map(
        lambda x: (x[0], x[2] if x[2] else x[1])).collectAsMap()
    avg_mtv_critic = [(artists_name[_id], 'Norway', sum(
        [float(s) for s in scores])/len(scores)
    ) for _id, scores in albums_from_norway]
    sorted_scores = sorted(avg_mtv_critic, key=lambda (name, _, avg): (-avg, name))
    save_as_tsv(sorted_scores, 'result_9.tsv')


task_9()


