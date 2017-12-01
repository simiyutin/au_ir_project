#! /usr/bin/python3

import glob
import json
import math
import os
import datetime

from collections import defaultdict

from project_dir import project_dir


# todo можно сделать быстрее, если использовать numpy
def idf(index, term, N):
    df = len(index.get(term, dict()).keys())  # количество докусентов, которые содержат данный терм
    return math.log(N / df)


def merge_weights(segregated_index):
    label_weights = {
        'h1': 2.,
        'h2': 1.,
        'h3': 1.,
        'h4': 1.,
        'h5': 1.,
        'body': 1.,
        'title': 2.,
        'tags': 2.,
        'query': 1.,
        'answer': 0.8
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


def calculate_bm_25_for_index(index, file_len_map, N, dlavg, additional_score, k1, b):
    bm25_index = dict()
    for word, docs in index.items():
        bm25_index[word] = dict()
        for doc, num in docs.items():
            fst = idf(index, word, N)
            tf = index.get(word, dict()).get(doc, 0)
            dl = file_len_map[doc]
            snd = (k1 + 1) * tf / (k1 * ((1 - b) + b * dl / dlavg) + tf)
            bm25_index[word][doc] = fst * snd * additional_score[doc]

    return bm25_index


def load_chunks(index_templ):
    index_chunks = []
    all_chunks = glob.glob(project_dir + index_templ)
    for filepath in all_chunks:
        with open(filepath, 'r') as file:
            index_chunks.append(json.load(file))
    return index_chunks


def get_number_of_elements(preproc_dir_name):
    return len(os.listdir(project_dir + preproc_dir_name))


def load_file_len_data(n_documents, file_len_name):
    file_len_map_path = project_dir + file_len_name
    with open(file_len_map_path, 'r') as fp:
        file_len_dict = json.load(fp)

    avg_file_len = 0
    for file_length in file_len_dict.values():
        avg_file_len += file_length

    avg_file_len //= n_documents

    return file_len_dict, avg_file_len


def do_bm(number_of_documents, input_index_templ, index_name_templ, file_len_name, additional_score: dict=defaultdict(lambda: 1.)):
    bm25_index_file_template = project_dir + index_name_templ

    index_chunks = load_chunks(input_index_templ)
    file_len_map, average_file_len = load_file_len_data(number_of_documents, file_len_name)

    for ind, segregated_index_chunk in enumerate(index_chunks):
        print('processing index chunk {} / {} ..'.format(ind + 1, len(index_chunks)))
        merged_index_chunk = merge_weights(segregated_index_chunk)
        bm25_index_chunk = calculate_bm_25_for_index(merged_index_chunk, file_len_map, number_of_documents,
                                                     average_file_len, additional_score, k1=1.2, b=0.75)

        with open(bm25_index_file_template.format(ind), 'w') as fp:
            json.dump(bm25_index_chunk, fp)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time (crawled): {}'.format(start_time))
    do_bm(get_number_of_elements('processed/'), "indexChunk*.txt", '/bm25IndexChunk{}.txt', 'fileLenMap.txt')
    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))

    start_time = datetime.datetime.now()
    print('start time (stackoverflow): {}'.format(start_time))
    score_info_file_name = "scorePostsMap.txt"
    with open(project_dir + score_info_file_name, 'r') as file:
        score_info: dict = json.load(file)
    score_min = min(score_info.values())
    score_max = max(score_info.values())

    def scale_score(v):
        return math.log(v - score_min + 1) / math.log(score_max)

    score_info = {k: 0.5 + scale_score(v) / 2. for (k, v) in score_info.items()}
    do_bm(get_number_of_elements('stackoverflow_processed/'), "stackIndexChunk*.txt", '/bm25StackIndexChunk{}.txt', 'stackLenMap.txt', score_info)
    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))

