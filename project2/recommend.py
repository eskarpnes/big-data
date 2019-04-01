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
    # scores = similarity_score(bags_of_words, user_bag)
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

    def count_words(words):
        words = words.split()
        words_dict = dict()
        for word in words:
            if word not in words_dict.keys():
                words_dict[word] = 1
            else:
                words_dict[word] += 1
        return words_dict

    tweets = rdd.reduceByKey(lambda a, b: a + b)

    bags = tweets.flatMapValues(lambda x: x.split())

    return bags


def extract_user(bags_of_words, user):
    user_bag = bags_of_words.filter(lambda x: x[0] == user)
    bags_of_words = bags_of_words.subtractByKey(user_bag)
    return bags_of_words, user_bag


def similarity_score(bags_of_words, user_bag):

    def sim(user_words, words):
        score = 0
        for word in user_words.keys():
            if word in words.keys():
                score += min(user_words[word], words[word])
        return score

    user_words = user_bag.collectAsMap().values()[0]
    user_scores = bags_of_words.mapValues(lambda words: sim(user_words, words))

    return user_scores


def sort_score(scores):
    pass


def get_top_scores(scores, k):
    pass


def print_top_scores(top_scores):
    pass


recommend("bradessex", 10, input="tweets_tiny.tsv")
