#! /usr/bin/python3

import datetime
import json
import os
from collections import Counter

from project_dir import project_dir


def count_words(entry):
    file_index, weight, words = entry
    return [(word, file_index, (weight, count)) for (word, count) in Counter(words).items()]


def add_to_index(index, entries):
    for entry in entries:
        word, file_index, (weight, count) = entry
        index_for_weight = index.get(weight, dict())
        cur_index_word_map = index_for_weight.get(word, dict())
        cur_index_word_map[file_index] = count
        index_for_weight[word] = cur_index_word_map
        index[weight] = index_for_weight


def flatten(listOfLists):
    return [item for itemlist in listOfLists for item in itemlist]


def get_file_len(file_entries):
    file_len = 0
    for _, _, terms in file_entries:
        file_len += len(terms)

    return file_len


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    preprocessed_dir = project_dir + 'processed/'
    index_file_template = project_dir + 'indexChunk{}.txt'
    file_len_map_path = project_dir + 'fileLenMap.txt'
    chunk_size = 20000

    index = dict()
    file_len_map = dict()

    all_files = os.listdir(preprocessed_dir)
    total = len(all_files)
    print('total files: {}'.format(total))
    processed = 0
    for ind, filename in enumerate(all_files):
        with open(preprocessed_dir + filename, 'r') as file:
            entries = json.load(file)
        if len(entries) > 0:
            file_index = int(entries[0][0])
            file_len_map[file_index] = get_file_len(entries)
            entries = map(count_words, entries)
            entries = flatten(entries)
            add_to_index(index, entries)

        processed += 1
        if processed % 100 == 0:
            print("processed: {} / {}".format(processed, total))

        if processed % chunk_size == 0 or ind == total - 1:
            print('saving chunk..')
            with open(index_file_template.format((processed - 1) // chunk_size), 'w') as fp:
                json.dump(index, fp)
            index = dict()

    with open(file_len_map_path, 'w') as fp:
        json.dump(file_len_map, fp)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
