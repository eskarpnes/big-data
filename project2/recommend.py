# coding: utf-8
from pyspark import SparkContext
import os

sc = SparkContext("local", "Big D")

'''
similarity to all users
select k users with max sim score
sort alphabetically
'''


def recommend(user, k=10, input="tweets.tsv", output="command_line"):
    # Loads all the data into an rdd
    rdd = get_rdd(input)
    # Creates bag of words for each user
    bags_of_words = create_bags_of_words(rdd)
    # Extracts user from bags of words
    bags_of_words, user_bag = extract_user(bags_of_words, user)
    # Scores the users based on bags of words
    scores = similarity_score(bags_of_words, user_bag)
    # sorted_scores = sort_score(scores)
    # top_scores = get_top_scores(scores, k)

    if output == "command_line":
        pass
        # print_top_scores(top_scores)
    else:
        pass
        # top_scores.saveAsTextFile(output)


def get_rdd(input):
    tweets = os.path.join(os.getcwd(), input)
    tweets_raw = sc.textFile(tweets)
    tweets_rdd = tweets_raw.map(lambda x: x.split('\t'))
    return tweets_rdd


def p(x):
    print x


def create_bags_of_words(rdd):

    bags = rdd.flatMapValues(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    return bags


def extract_user(bags_of_words, user):
    user_bag = bags_of_words.filter(lambda x: x[0][0] == user)
    bags_of_words = bags_of_words.subtractByKey(user_bag)
    return bags_of_words, user_bag


def similarity_score(bags_of_words, user_bag):
    # result = bags_of_words.cartesian(user_bag).filter(lambda x: x[0][0][1] == x[1][0][1]).map(lambda x: (x[0][0][0], min(x[0][1], x[1][1]))).reduceByKey(lambda a, b: a + b)
    
    result.foreach(p)

    return None


def sort_score(scores):
    pass


def get_top_scores(scores, k):
    pass


def print_top_scores(top_scores):
    pass


recommend("mary", 2, input="example.tsv")
