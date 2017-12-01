#! /usr/bin/python3

from project_dir import project_dir
import os, glob, json
from processer import process_text
from search_results_factory import node, search_results
from bs4 import BeautifulSoup


def concat_chunks(acceptor, donor):
    for word, docs in donor.items():
        if word not in acceptor.keys():
            acceptor[word] = dict()
        for doc, num in docs.items():
            acceptor[word][doc] = num


def get_best_snippet(words, processed_query, snippet_word_len):
    all_snippets_num = len(words) // snippet_word_len
    best_i = 0
    best_common = 0
    for i in range(all_snippets_num):
        snippet = words[i * snippet_word_len: (i + 1) * snippet_word_len]
        processed_snippet = process_text(' '.join(snippet))
        common = 0
        for term in processed_snippet:
            if term in processed_query:
                common += 1
        if common > best_common:
            best_common = common
            best_i = i

    best_snippet = words[best_i * snippet_word_len: (best_i + 1) * snippet_word_len]

    for i in range(len(best_snippet)):
        processed_term_array = process_text(best_snippet[i])
        if len(processed_term_array) > 0 and processed_term_array[0] in processed_query:
            best_snippet[i] = '<b>{}</b>'.format(best_snippet[i])

    return ' '.join(best_snippet)


def get_snippet(file_path, processed_query, snippet_word_len):
    with open(file_path, 'r') as original_file:
        html = ''.join(original_file.readlines()[1:])  # skip link
    parsed_page = BeautifulSoup(html, "lxml")  # todo перенести эту часть в препроцессер
    body = parsed_page.find('body')
    body_text = body.text if body is not None else ""
    body_text = body_text.replace('\n', ' ').split(' ')
    snippet = get_best_snippet(body_text, processed_query, snippet_word_len)
    return snippet


def map_to_result_node(index_files_map, p, query):
    doc_index, bm25_score = p
    title, link, original_name = index_files_map[doc_index]
    if title == "":
        title = link
    snippet = get_snippet(project_dir + '/crawled/' + original_name, query, 50)
    return node(title, link, snippet)


class SearchEngine:
    def __init__(self):
        all_chunks = glob.glob(project_dir + "bm25IndexChunk*.txt")
        self.index = dict()
        for ind, filepath in enumerate(all_chunks):
            print('processing {} / {} chunk..'.format(ind + 1, len(all_chunks)))
            with open(filepath, 'r') as file:
                chunk = json.load(file)
            concat_chunks(self.index, chunk)

        with open(project_dir + 'indexFilesMap.txt') as fp:
            self.index_files_map = json.load(fp)

        print('ready to take queries')

    def ask(self, query):
        processed_query = process_text(query)
        ranked_documents = dict()
        for term in processed_query:
            docs = self.index.get(term, dict())
            for doc, doc_score in docs.items():
                prev_score = ranked_documents.get(doc, 0)
                ranked_documents[doc] = prev_score + doc_score

        ranked_documents_list = ranked_documents.items()
        ranked_documents_list = sorted(ranked_documents_list, key=lambda it: it[1], reverse=True)

        def map_to_node_function(p):
            return map_to_result_node(self.index_files_map, p, processed_query)

        ranked_documents_list = ranked_documents_list[:20]
        ranked_documents_list = list(map(map_to_node_function, ranked_documents_list))

        sovf_best = node('dummy sovf title', 'stackoverflow.com', 'dummy sovf snippet')

        results = search_results(ranked_documents_list, sovf_best)
        return results


if __name__ == '__main__':
    searchEngine = SearchEngine()

    query = 'binary search'
    while True:
        results = searchEngine.ask(query)

        print('query: {}'.format(query))
        print('documents: {}'.format(results))
        query = input('enter new query:')
