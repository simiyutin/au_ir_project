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
import enchant

import datetime

import pickle
import multiprocessing as mp
import numpy as np

from project_dir import project_dir

headers = ['h{}'.format(i) for i in range(1, 6)]  # h1, h2, h3 ...


def process_text(text):
    translator = str.maketrans('', '', string.punctuation)
    nopunct = text.translate(translator)
    tokenised = nopunct.lower().split()
    english = [w for w in tokenised if dictionary.check(w)]
    nostops = [w for w in english if w not in stop]
    stemmed = [ps.stem(w) for w in nostops]
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


# entry format: (file name, source tag, text)
def file_to_entries(file_name, text):
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


def process_chunk(chunk, process_id):
    total = len(chunk)
    processed = 0
    for pagefile in chunk:
        with open(crawled_dir + pagefile, 'r') as page:
            lines = "\n".join(page.readlines())
            entries = file_to_entries(pagefile, lines)
            processed_entries = [language_process(entry) for entry in entries]
            with open(processed_dir + pagefile, 'wb') as pr:
                pickle.dump(processed_entries, pr)

        processed += 1
        if processed % 100 == 0:
            print("process id={}, processed: {} / {}".format(process_id, processed, total))


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    # nltk.download() # при первом запуске раскомменчиваешь и выбираешь вкладку corpora -> stopwords и грузишь пакет
    stop = set(stopwords.words('english'))  # .union(stopwords.words('russian')) # хотим ли иметь дело с русским языком?
    ps = PorterStemmer()
    dictionary = enchant.Dict("en_US")

    crawled_dir = project_dir + 'crawled_renamed/'
    processed_dir = project_dir + 'crawled_processed/'

    all_page_files = os.listdir(crawled_dir)

    ncores = 8
    chunks = np.array_split(all_page_files, ncores)
    pool = mp.Pool(processes=ncores)
    results = [pool.apply_async(process_chunk, args=(chunk, process_id)) for process_id, chunk in enumerate(chunks)]
    results = [p.get() for p in results]

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
    # на моей машине 75к примерно 1 час 10 минут
