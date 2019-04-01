# coding: utf-8
from pyspark import SparkContext
import os
import time
import argparse

sc = SparkContext("local", "Big D")

'''
similarity to all users
select k users with max sim score
sort alphabetically
'''

parser = argparse.ArgumentParser(description="A recommender")
parser.add_argument("-k", default=10, type=int, help="Number of similar users to return")
parser.add_argument("-user", default="bradessex", help="User to find similar users from")
parser.add_argument("-file", default="tweets.tsv", help="Input file path")
parser.add_argument("-output", default="command_line", help="Output file path")

args = parser.parse_args()

def recommend(user, k, input, output):
    # Loads all the data into an rdd
    rdd = get_rdd(input)
    start = time.time()
    # Creates bag of words for each user
    bags_of_words = create_bags_of_words(rdd, user)
    # Extracts user from bags of words
    bags_of_words, user_bag = extract_user(bags_of_words, user)
    # Scores the users based on bags of words
    scores = similarity_score(bags_of_words, user_bag)
    sorted_scores = sort_score(scores)
    top_scores = get_top_scores(sorted_scores, k)

    end = time.time()

    print "time elapsed: "
    print end-start

    if output == "command_line":
        print_top_scores(top_scores)
    else:
        top_scores = sc.parallelize(top_scores)
        save(top_scores, output)


def save(data, path):
    data = data.map(lambda x: "\t".join([str(x[0]), str(x[1])]))
    data.saveAsTextFile(os.path.join(os.getcwd(), path))


def get_rdd(input):
    tweets = os.path.join(os.getcwd(), input)
    tweets_raw = sc.textFile(tweets)
    tweets_rdd = tweets_raw.map(lambda x: x.split('\t'))
    return tweets_rdd


def p(x):
    print x


def create_bags_of_words(rdd, user):
    user_words = rdd.filter(lambda x: x[0] == user)\
        .flatMapValues(lambda x: x.split())\
        .map(lambda x: x[1]).distinct().collect()
    bags = rdd.flatMapValues(lambda x: x.split())\
        .filter(lambda x: x[1] in user_words)\
        .map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    return bags


def extract_user(bags_of_words, user):
    user_bag = bags_of_words.filter(lambda x: x[0][0] == user)
    bags_of_words = bags_of_words.subtractByKey(user_bag)
    return bags_of_words, user_bag


def similarity_score(bags_of_words, user_bag):

    # Rearrange tuples so words are keys
    def rearrange_tuple(x):
        return x[0][1], (x[0][0], x[1])

    bags_of_words = bags_of_words.map(rearrange_tuple)
    user_bag = user_bag.map(rearrange_tuple)

    def word_sim(data):
        other_user = data[1][0][0]
        other_score = data[1][0][1]
        user_score = data[1][1][1]
        return other_user, min(other_score, user_score)

    result = bags_of_words.join(user_bag).map(word_sim).reduceByKey(lambda a, b: a + b)

    return result


def sort_score(scores):
    scores = scores.sortByKey().sortBy(lambda x: x[1], False)
    return scores


def get_top_scores(scores, k):
    return scores.take(k)


def print_top_scores(top_scores):
    for score in top_scores:
        print score


recommend(args.user, args.k, args.file, args.output)
