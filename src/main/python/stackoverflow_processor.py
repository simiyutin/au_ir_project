#! /usr/bin/python3

import os
from os import getenv

import pymssql

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

import string

import datetime

import pickle
import multiprocessing as mp
import numpy as np

import json

from project_dir import project_dir

from processer import process_text


def process_tags(text):
    translator = str.maketrans('<>', '  ')
    return process_text(text.translate(translator))


# entry format: (post_id, source tag, text)
def process(file_index, entry):
    (title, tags, query, answer) = entry

    return [(file_index, "title", process_text(title)),
            (file_index, "tags", process_tags(tags)),
            (file_index, "query", process_text(query)),
            (file_index, "answer", process_text(answer))]


def process_chunk(chunk, file_name_shift, process_id):
    connection = pymssql.connect(server, user, password, "stackDB")
    cursor = connection.cursor()
    total = len(chunk)
    processed = 0
    results = []
    for (id, accepted_id, score) in chunk:

        cursor.execute(f"""
        select
            q.Title,
            q.Tags,
            q.Body
        from Posts q
        where q.Id = {id}
        """)

        (title, tags, query) = cursor.fetchone()

        cursor.execute(f"""
        select
            p.Body
        from Posts p
        where p.Id = {accepted_id}
        """)

        (answer,) = cursor.fetchone()

        entry = (title, tags, query, answer)

        file_index = processed + file_name_shift
        processed_entries = process(file_index, entry)

        output_name = processed_dir + '{}.txt'.format(file_index)

        results.append((id, score))

        with open(output_name, 'w', encoding='utf-8') as pr:
            json.dump(processed_entries, pr)

        processed += 1
        if processed % 100 == 0:
            print("process id={}, processed: {} / {}".format(process_id, processed, total))

    connection.close()
    return results


def get_data():
    conn = pymssql.connect(server, user, password, "stackDB")

    cursor = conn.cursor()

    cursor.execute("""
    select top 30000
        q.Id as id,
        q.AcceptedAnswerId as acceptedId,
        p.Score as score
    from Posts q
    inner join Posts p on q.AcceptedAnswerId = p.Id
    where p.Score > 0
    order by p.Score desc
    """)

    data = cursor.fetchall()

    # with open(project_dir + "postsIds.txt", 'w') as of:
    #     json.dump(data, of)

    conn.close()
    return data


def process_data(data):
    if os.path.exists(processed_dir):
        import shutil
        shutil.rmtree(processed_dir)

    os.mkdir(processed_dir)

    ncores = 4
    chunks = np.array_split(data, ncores)
    shifts = [0]
    for i in range(1, ncores):
        shifts.append(shifts[i - 1] + len(chunks[i - 1]))

    pool = mp.Pool(processes=ncores)
    futures = [pool.apply_async(process_chunk, args=(chunk, shifts[process_id], process_id)) for process_id, chunk in enumerate(chunks)]
    results = [p.get() for p in futures]

    ids_map = dict()
    scores_map = dict()
    for process_id, chunk in enumerate(results):
        for ind, res in enumerate(chunk):
            id, score = res
            ids_map[shifts[process_id] + ind] = int(id)
            scores_map[shifts[process_id] + ind] = int(score)

    print('saving ids map..')
    with open(project_dir + "indexPostsMap.txt", 'w+', encoding='utf-8') as of:
        json.dump(ids_map, of)

    print('saving score map..')
    with open(project_dir + "scorePostsMap.txt", 'w+', encoding='utf-8') as of:
        json.dump(scores_map, of)


if __name__ == '__main__':
    server = getenv("MS_SERVER")
    user = getenv("MS_USERNAME")
    password = getenv("MS_PASSWORD")

    processed_dir = project_dir + 'stackoverflow_processed/'

    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    data = get_data()

    intermid_time = datetime.datetime.now()
    print('ids collected in: {}'.format(intermid_time - start_time))

    process_data(data)

    end_time = datetime.datetime.now()
    print('preprocessed in: {}'.format(end_time - intermid_time))



