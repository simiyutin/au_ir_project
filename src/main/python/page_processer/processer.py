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
            body = BeautifulSoup(lines, "lxml").find('body')
            body_text = body.text
            translator = str.maketrans('', '', string.punctuation)
            nopunct = body_text.translate(translator)
            tokenised = nopunct.lower().split()
            english = [w for w in tokenised if d.check(w)]
            nostops = [w for w in english if w not in stop]
            stemmed = [ps.stem(w) for w in nostops]
            output_data = " ".join(stemmed)
            with open(output_dir + "processed_" + pagefile, 'w+') as resfile:
                resfile.write(output_data)

        processed += 1
        print("processed: {} / {}".format(processed, total))
