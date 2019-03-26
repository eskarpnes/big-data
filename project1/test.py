import pyspark

sc = pyspark.SparkContext('local', 'BigD')

album_raw = sc.textFile("data/albums.csv")
album_rdd = album_raw.map(lambda x: x.split(","))

artist_raw = sc.textFile("data/artists.csv")
artist_rdd = artist_raw.map(lambda x: x.split(","))


def task_1():
    print(album_rdd.map(lambda x: x[3]).distinct().count())

# task_1()


def task_2():
    print(artist_rdd.sortBy(lambda x: x[4]).first()[4])

# task_2()


def task_3():
    countries = artist_rdd.map(lambda x: x[5]).countByValue()
    countries_sorted = sc.parallelize([x for x in sorted(countries.items(), key=lambda (k, v): (-v, k))])
    countries_sorted.map(lambda (k, v): k.encode('utf-8') + "\t" + str(v)).saveAsTextFile("results/result_3")

# task_3()


def task_4():
    albums = album_rdd.map(lambda x: x[1]).countByValue()
    albums_sorted = sc.parallelize([x for x in sorted(albums.items(), key=lambda (k, v): (-v, int(k)))])
    albums_sorted.map(lambda (k, v): k + "\t" + str(v)).saveAsTextFile("results/result_4")

# task_4()


def task_5():
    sales = album_rdd.map(lambda x: (x[3], x[6]))
    sales = sales.reduceByKey(lambda a, b: int(a)+int(b)).collectAsMap()
    sales_sorted = sc.parallelize([x for x in sorted(sales.items(), key=lambda (k, v): (-v, k))])
    sales_sorted.map(lambda (k, v): k + "\t" + str(v)).saveAsTextFile("results/result_5")


# task_5()

def task_6():
    critic_score = album_rdd.map(lambda x: (x[0], (float(x[7])+float(x[8])+float(x[9]))/3)).takeOrdered(10, key=lambda x: -x[1])
    print(critic_score)


task_6()
