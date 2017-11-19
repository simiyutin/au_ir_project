import json
from pyspark.sql import SparkSession
from collections import Counter
import datetime
import os
import pickle
import io
from project_dir import project_dir


def extract_entries(bytes):
    file = io.BytesIO(bytes[1])
    result = pickle.load(file)
    return result


def count_words(entry):
    file_name, weight, words = entry
    return [(word, file_name, (weight, count)) for (word, count) in Counter(words).items()]


def add_to_index(index, entries):
    for entry in entries:
        word, file_name, (weight, count) = entry
        cur_index_word_map = index.get(word, dict())
        cur_index_file_map = cur_index_word_map.get(file_name, dict())
        cur_index_file_map[weight] = count
        cur_index_word_map[file_name] = cur_index_file_map
        index[word] = cur_index_word_map


def flatten(listOfLists):
    return [item for itemlist in listOfLists for item in itemlist]


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    preprocessed_dir = project_dir + 'crawled_processed/'
    index_file_template = project_dir + 'index{}.pkl'
    chunk_size = 20000

    index = dict()
    all_files = os.listdir(preprocessed_dir)
    total = len(all_files)
    print('total files: {}'.format(total))
    processed = 0
    for filename in all_files:
        with open(preprocessed_dir + filename, 'rb') as file:
            entries = pickle.load(file)
            entries = map(count_words, entries)
            entries = flatten(entries)
            add_to_index(index, entries)

        processed += 1
        if processed % 100 == 0:
            print("processed: {} / {}".format(processed, total))

        if processed % chunk_size == 0:
            print('saving chunk..')
            with open(index_file_template.format((processed - 1) // chunk_size), 'wb') as fp:
                pickle.dump(index, fp)
            index = dict()

    print('saving chunk..')
    with open(index_file_template.format((processed - 1) // chunk_size), 'wb') as fp:
        pickle.dump(index, fp)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))



    # spark = SparkSession.builder.appName("").master("local[*]").getOrCreate()
    # sc = spark.sparkContext
    # entries = []
    # res = sc\
    #     .binaryFiles(preprocessed_dir)\
    #     .flatMap(extract_entries)\
    #     .flatMap(count_words)\
    #     .groupBy(lambda entry: (entry[0], entry[1])) \
    #     .map(lambda entry: (entry[0], [counters for (wo, we, counters) in entry[1]])) \
    #     .groupBy(lambda entry: entry[0][0]) \
    #     .map(lambda entry: (entry[0], {file_name: dict(values) for ((word, file_name), values) in entry[1]})) \
    #     .collect()
    #
    # res = dict(res)
    # with open(index_file, 'w') as fp:
    #     json.dump(res, fp)
    #
    # end_time = datetime.datetime.now()
    # print('time elapsed: {}'.format(end_time - start_time))
