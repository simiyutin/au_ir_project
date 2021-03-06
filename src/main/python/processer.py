#! /usr/bin/python3

import os

from bs4 import BeautifulSoup, Comment
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

header_tags = ['h{}'.format(i) for i in range(1, 6)]  # h1, h2, h3 ...

# todo allow all printable characters. e.g. we need to support c++ as query term
def only_letters_and_digits(tested_string):
    match = re.match("^[a-z0-9]*$", tested_string)
    return match is not None


# nltk.download() # при первом запуске раскомменчиваешь и выбираешь вкладку corpora -> stopwords и грузишь пакет
stop = set(stopwords.words('english'))  # .union(stopwords.words('russian')) # хотим ли иметь дело с русским языком?
ps = PorterStemmer()


def process_text(text):
    # translator = str.maketrans('', '', string.punctuation)
    # nopunct = text.translate(translator)
    # tokenised = nopunct.lower().split()
    # english = [w for w in tokenised if only_letters_and_digits(w)]
    tokenised = text.lower().split()
    nostops = [w for w in tokenised if w not in stop]
    stemmed = [ps.stem(w) for w in nostops]
    return stemmed


def get_headers_nodes(parent, file_index):
    if parent is None:
        return []

    return [(file_index, h, " ".join(one.text.strip() for one in parent.find_all(h))) for h in
            header_tags]


def delete_headers(parent):
    if parent is None:
        return

    for header in parent.find_all(header_tags):
        header.decompose()


def delete_scripts(parent):
    if parent is None:
        return

    for script in parent.find_all('script'):
        script.decompose()


def delete_comments(parent):
    if parent is None:
        return

    for element in parent(text=lambda text: isinstance(text, Comment)):
        element.extract()


def get_body_nodes(parent, file_index):
    if parent is None:
        return []

    return [(file_index, "body", parent.text)]


def get_title(parent):
    if parent is None:
        return ""

    title = parent.find('title')
    return title.text if title is not None else ""


# entry format: (file name, source tag, text)
def parse_html(file_index, text):
    parsed_page = BeautifulSoup(text, "lxml")
    title = get_title(parsed_page)
    body = parsed_page.find('body')

    delete_scripts(body)
    delete_comments(body)

    plain_text = "" if body is None else body.text

    headers_nodes = get_headers_nodes(body, file_index)
    # кажется, что нахождение заголовков сразу в двух местах ничего не испортит,
    # но тогда тело всегда будет давать ненулевой вклад в ранг документа,
    # даже если оно состоит только из одних заголовков
    delete_headers(body)

    body_nodes = get_body_nodes(body, file_index)
    return title, plain_text, headers_nodes + body_nodes


def language_process(entry):
    document_index, weight, text = entry
    return document_index, weight, process_text(text)


def process_chunk(chunk, file_name_shift, process_id):
    total = len(chunk)
    processed = 0
    metadata = []
    for pagefile in chunk:
        with open(crawled_dir + pagefile, 'r', encoding='utf-8') as page:
            file_index = processed + file_name_shift
            lines = page.readlines()
            link = lines[0]
            lines = "\n".join(lines[1:])
            title, plain_text_page, entries = parse_html(file_index, lines)
            processed_entries = [language_process(entry) for entry in entries]
            output_name = processed_dir + '{}.txt'.format(file_index)
            metadata.append((title, link, pagefile))
            with open(output_name, 'w', encoding='utf-8') as pr:
                json.dump(processed_entries, pr)
            output_plain_text_name = plain_files_dir + '{}.txt'.format(file_index)
            with open(output_plain_text_name, 'w', encoding='utf-8') as pr:
                pr.write(plain_text_page)

        processed += 1
        if processed % 100 == 0:
            print("process id={}, processed: {} / {}".format(process_id, processed, total))

    return metadata


def refresh(dir):
    if os.path.exists(dir):
        import shutil
        shutil.rmtree(dir)

    os.mkdir(dir)


if __name__ == '__main__':
    # onlamp = open('/media/boris/Data/shared/au_3/ir/project_recrawl_small/crawled/http:__www.onlamp.com_pub_q_all_python_articles_-1265112330.txt', 'r')
    # lines = "\n".join(onlamp.readlines()[1:])
    # page = BeautifulSoup(lines, "lxml")
    # body = page.find('body')
    # delete_scripts(body)
    # delete_comments(body)
    # print(body.prettify())
    # exit(0)


    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    crawled_dir = project_dir + 'crawled/'
    processed_dir = project_dir + 'processed/'
    plain_files_dir = project_dir + 'plain_text_unprocessed/'

    refresh(processed_dir)
    refresh(plain_files_dir)

    all_page_files = os.listdir(crawled_dir)

    ncores = 8
    chunks = np.array_split(all_page_files, ncores)
    shifts = [0]
    for i in range(1, ncores):
        shifts.append(shifts[i - 1] + chunks[i - 1].size)

    pool = mp.Pool(processes=ncores)
    futures = [pool.apply_async(process_chunk, args=(chunk, shifts[process_id], process_id)) for process_id, chunk in
               enumerate(chunks)]
    metadata = [p.get() for p in futures]

    filesMap = dict()
    for process_id, chunk in enumerate(metadata):
        for ind, link_title in enumerate(chunk):
            filesMap[shifts[process_id] + ind] = link_title

    print('saving file names map..')
    with open(project_dir + "indexFilesMap.txt", 'w+', encoding='utf-8') as of:
        json.dump(filesMap, of)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
    # на моей машине 75к примерно 1 час 10 минут
