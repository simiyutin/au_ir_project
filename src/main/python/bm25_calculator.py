#! /usr/bin/python3

import glob
import json
import math
import os
import datetime

from project_dir import project_dir


# todo можно сделать быстрее, если использовать numpy
def idf(index, term, N):
    df = len(index.get(term, dict()).keys())  # количество докусентов, которые содержат данный терм
    return math.log(N / df)


def merge_weights(segregated_index):
    label_weights = {
        'h1': 2,
        'h2': 1,
        'h3': 1,
        'h4': 1,
        'h5': 1,
        'body': 1,
    }

    merged_index = dict()

    for label, index in segregated_index.items():
        for word, docs in index.items():
            if word not in merged_index.keys():
                merged_index[word] = dict()
            for doc, num in docs.items():
                if doc not in merged_index[word].keys():
                    merged_index[word][doc] = 0

                merged_index[word][doc] += num * label_weights[label]

    return merged_index


def calculate_bm_25_for_index(index, file_len_map, N, dlavg, k1, b):
    bm25_index = dict()
    for word, docs in index.items():
        bm25_index[word] = dict()
        for doc, num in docs.items():
            fst = idf(index, word, N)
            tf = index.get(word, dict()).get(doc, 0)
            dl = file_len_map[doc]
            snd = (k1 + 1) * tf / (k1 * ((1 - b) + b * dl / dlavg) + tf)
            bm25_index[word][doc] = fst * snd

    return bm25_index


def load_chunks():
    index_chunks = []
    all_chunks = glob.glob(project_dir + "indexChunk*.txt")
    for filepath in all_chunks:
        with open(filepath, 'r', encoding='utf-8') as file:
            index_chunks.append(json.load(file))
    return index_chunks


def get_number_of_elements():
    return len(os.listdir(project_dir + 'processed/'))


def load_file_len_data(n_documents):
    file_len_map_path = project_dir + 'fileLenMap.txt'
    with open(file_len_map_path, 'r', encoding='utf-8') as fp:
        file_len_dict = json.load(fp)

    avg_file_len = 0
    for file_length in file_len_dict.values():
        avg_file_len += file_length

    avg_file_len //= n_documents

    return file_len_dict, avg_file_len


if __name__ == '__main__':

    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    bm25_index_file_template = project_dir + '/bm25IndexChunk{}.txt'

    index_chunks = load_chunks()
    number_of_documents = get_number_of_elements()
    file_len_map, average_file_len = load_file_len_data(number_of_documents)

    for ind, segregated_index_chunk in enumerate(index_chunks):
        print('processing index chunk {} / {} ..'.format(ind + 1, len(index_chunks)))
        merged_index_chunk = merge_weights(segregated_index_chunk)
        bm25_index_chunk = calculate_bm_25_for_index(merged_index_chunk, file_len_map, number_of_documents,
                                                     average_file_len, k1=1.2, b=0.75)

        with open(bm25_index_file_template.format(ind), 'w', encoding='utf-8') as fp:
            json.dump(bm25_index_chunk, fp)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
