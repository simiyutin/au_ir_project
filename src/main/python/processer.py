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
# sudo pip install lxml
# а потом я долго и уныло долбалась со спарком и хадупом
# sudo pip install pyspark
# спарк ругался на hadoop-native, поэтому пришлось прописать его в LD_LIBRARY_PATH

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

import string

import datetime

import pickle
import multiprocessing as mp
import numpy as np

import json

import re

from project_dir import project_dir

headers = ['h{}'.format(i) for i in range(1, 6)]  # h1, h2, h3 ...


def only_letters_and_digits(tested_string):
    match = re.match("^[a-z0-9]*$", tested_string)
    return match is not None


def process_text(text):
    translator = str.maketrans('', '', string.punctuation)
    nopunct = text.translate(translator)
    tokenised = nopunct.lower().split()
    english = [w for w in tokenised if only_letters_and_digits(w)]
    nostops = [w for w in english if w not in stop]
    stemmed = [ps.stem(w) for w in nostops]
    return stemmed


def get_headers(parsed_page, file_index):
    return [(file_index, h, " ".join(one.text.strip() for one in parsed_page.find_all(h))) for h in
            headers]


def delete_headers(parsed_page):
    for header in parsed_page.find_all(headers):
        header.decompose()


def get_body(parsed_page, file_index):
    body = parsed_page.find('body')
    if body is None:
        return []
    else:
        return [(file_index, "body", body.text)]


# entry format: (file name, source tag, text)
def file_to_entries(file_index, text):
    parsed_page = BeautifulSoup(text, "lxml")
    headers = get_headers(parsed_page, file_index)
    # кажется, что нахождение заголовков сразу в двух местах ничего не испортит,
    # но тогда тело всегда будет давать ненулевой вклад в ранг документа,
    # даже если оно состоит только из одних заголовков
    delete_headers(parsed_page)
    body = get_body(parsed_page, file_index)
    return headers + body


def language_process(entry):
    document_index, weight, text = entry
    return document_index, weight, process_text(text)


def process_chunk(chunk, file_name_shift, process_id):
    total = len(chunk)
    processed = 0
    links = []
    for pagefile in chunk:
        with open(crawled_dir + pagefile, 'r') as page:
            file_index = processed + file_name_shift
            lines = page.readlines()
            link = lines[0]
            lines = "\n".join(lines[1:])
            entries = file_to_entries(file_index, lines)
            processed_entries = [language_process(entry) for entry in entries]
            output_name = processed_dir + '{}.txt'.format(file_index)
            links.append(link)
            with open(output_name, 'w') as pr:
                json.dump(processed_entries, pr)

        processed += 1
        if processed % 100 == 0:
            print("process id={}, processed: {} / {}".format(process_id, processed, total))

    return links


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    # nltk.download() # при первом запуске раскомменчиваешь и выбираешь вкладку corpora -> stopwords и грузишь пакет
    stop = set(stopwords.words('english'))  # .union(stopwords.words('russian')) # хотим ли иметь дело с русским языком?
    ps = PorterStemmer()

    crawled_dir = project_dir + 'crawled/'
    processed_dir = project_dir + 'processed/'

    all_page_files = os.listdir(crawled_dir)

    ncores = 8
    chunks = np.array_split(all_page_files, ncores)
    shifts = [0]
    for i in range(1, ncores):
        shifts.append(shifts[i - 1] + chunks[i - 1].size)
    
    pool = mp.Pool(processes=ncores)
    futures = [pool.apply_async(process_chunk, args=(chunk, shifts[process_id], process_id)) for process_id, chunk in enumerate(chunks)]
    links = [p.get() for p in futures]

    filesMap = dict()
    for process_id, chunk in enumerate(links):
        for ind, link in enumerate(chunk):
            filesMap[shifts[process_id] + ind] = link

    print('saving file names map..')
    with open(project_dir + "indexFilesMap.txt", 'w+') as of:
        json.dump(filesMap, of)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
    # на моей машине 75к примерно 1 час 10 минут
