#! /usr/bin/python3

import os

from bs4 import BeautifulSoup
# может понадобиться
# sudo pip3 install --upgrade beautifulsoup4
# sudo pip3 install --upgrade html5lib

# напишу здесь что мне пришлось установить на всякий случай
# sudo pip install beautifulsoup4
# sudo pip install html5lib
# sudo pip install nltk
# sudo pip install pyenchant
# pacaur -S aspell-en
# sudo pip install lxml
# а потом я долго и уныло долбалась со спарком и хадупом
# поставила apache-spark, hadoop
# sudo pip install pyspark
# спарк ругался на hadoop-native, поэтому пришлось прописать его в LD_LIBRARY_PATH

from nltk.corpus import stopwords
from nltk import word_tokenize

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

import string
import enchant
import json

from pyspark.sql import SparkSession
from collections import Counter

headers = ['h{}'.format(i) for i in range(1, 6)]  # h1, h2, h3 ...


def process_text(text):
    translator = str.maketrans('', '', string.punctuation)
    nopunct = text.translate(translator)
    tokenised = nopunct.lower().split()
    english = [w for w in tokenised if broadcast_dictionary.value.check(w)]
    nostops = [w for w in english if w not in broadcast_stop.value]
    stemmed = [broadcast_ps.value.stem(w) for w in nostops]
    return stemmed


def get_headers(parsed_page, file_name):
    return [(file_name, h, " ".join(one.text.strip() for one in parsed_page.find_all(h))) for h in headers]


def delete_headers(parsed_page):
    for header in parsed_page.find_all(headers):
        header.decompose()


def get_body(parsed_page, file_name):
    body = parsed_page.find('body')
    if body is None:
        return []
    else:
        return [(file_name, "body", body.text)]


def file_to_text(file):
    path, text = file
    file_name = os.path.basename(path)
    parsed_page = BeautifulSoup(text, "lxml")
    headers = get_headers(parsed_page, file_name)

    # кажется, что нахождение заголовков сразу в двух местах ничего не испортит,
    # но тогда тело всегда будет давать ненулевой вклад в ранг документа,
    # даже если оно состоит только из одних заголовков
    delete_headers(parsed_page)

    body = get_body(parsed_page, file_name)
    return headers + body


def language_process(entry):
    file_name, weight, text = entry
    return file_name, weight, process_text(text)


def count_words(entry):
    file_name, weight, words = entry
    return [(word, file_name, (weight, count)) for (word, count) in Counter(words).items()]


if __name__ == '__main__':

    # nltk.download() # при первом запуске раскомменчиваешь и выбираешь вкладку corpora -> stopwords и грузишь пакет
    stop = set(stopwords.words('english'))  # .union(stopwords.words('russian')) # хотим ли иметь дело с русским языком?
    ps = PorterStemmer()
    dictionary = enchant.Dict("en_US")

    # crawled_dir = '../../../../crawled/'
    crawled_dir = '../../../../crawled_processed_for_index/'  # renamed
    # crawled_dir = '../../../../small_crawled/'  # todo: change crawled directory
    # crawled_dir = '/media/boris/Data/shared/au_3/ir/project/crawled_1'

    print('total files: {}'.format(len(os.listdir(crawled_dir))))

    index_file = '../../../../index.txt'

    spark = SparkSession.builder.appName("").master("local").getOrCreate()
    sc = spark.sparkContext

    broadcast_stop = sc.broadcast(stop)
    broadcast_ps = sc.broadcast(ps)
    broadcast_dictionary = sc.broadcast(dictionary)

    res = \
        sc\
        .wholeTextFiles(crawled_dir)\
        .flatMap(file_to_text)\
        .map(language_process) \
        .flatMap(count_words) \
        .groupBy(lambda entry: (entry[0], entry[1])) \
        .map(lambda entry: (entry[0], [counters for (wo, we, counters) in entry[1]]))\
        .groupBy(lambda entry: entry[0][0]) \
        .map(lambda entry: (entry[0], {file_name: dict(values) for ((word, file_name), values) in entry[1]}))\
        .collect()

    res = dict(res)
    with open(index_file, 'w') as fp:
        json.dump(res, fp)
