import os

from bs4 import BeautifulSoup
# может понадобиться
# sudo pip3 install --upgrade beautifulsoup4
# sudo pip3 install --upgrade html5lib

from nltk.corpus import stopwords
from nltk import word_tokenize

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

import string

import enchant

import json


def process_text(text):
    translator = str.maketrans('', '', string.punctuation)
    nopunct = text.translate(translator)
    tokenised = nopunct.lower().split()
    english = [w for w in tokenised if d.check(w)]
    nostops = [w for w in english if w not in stop]
    stemmed = [ps.stem(w) for w in nostops]
    output_data = " ".join(stemmed)
    return output_data


def dump_headers():
    for i in range(1, 6):
        h = 'h{}'.format(i)  # h1, h2, h3 ...
        page_data[h] = []
        for header in parsed_page.find_all(h):
            page_data[h].append(header.text)


def delete_headers():
    for header in parsed_page.find_all(['h{}'.format(i) for i in range(1, 6)]):
        header.decompose()


def dump_body():
    page_data['body'] = [parsed_page.find('body').text]


def serialize():
    return json.dumps(page_data)


def language_process():
    for key, values in page_data.items():
        processed_values = []
        for value in values:
            processed_values.append(process_text(value))
        page_data[key] = processed_values


if __name__ == '__main__':
    # nltk.download() # при первом запуске раскомменчиваешь и выбираешь вкладку corpora -> stopwords и грузишь пакет
    stop = set(stopwords.words('english'))  # .union(stopwords.words('russian')) # хотим ли иметь дело с русским языком?
    ps = PorterStemmer()
    d = enchant.Dict("en_US")
    crawled_dir = '../../../../crawled/'
    output_dir = '../../../../preprocessed/'
    all_page_files = os.listdir(crawled_dir)
    total = len(all_page_files)
    processed = 0
    for pagefile in all_page_files:
        with open(crawled_dir + pagefile, 'r') as page:
            lines = " ".join(page.readlines())
            parsed_page = BeautifulSoup(lines, "lxml")
            page_data = dict()
            dump_headers()

            # кажется, что нахождение заголовков сразу в двух местах ничего не испортит,
            # но тогда тело всегда будет давать ненулевой вклад в ранг документа,
            # даже если оно состоит только из одних заголовков
            delete_headers()

            dump_body()
            language_process()
            page_text = serialize()
            with open(output_dir + "processed_" + pagefile, 'w+') as resfile:
                resfile.write(page_text)

        processed += 1
        print("processed: {} / {}".format(processed, total))
