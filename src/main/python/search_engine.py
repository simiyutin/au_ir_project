#! /usr/bin/python3

from project_dir import project_dir
import os, glob, json
from processer import process_text
from search_results_factory import node, search_results


def concat_chunks(acceptor, donor):
    for word, docs in donor.items():
        if word not in acceptor.keys():
            acceptor[word] = dict()
        for doc, num in docs.items():
            acceptor[word][doc] = num


def get_snippet(file_path, processed_query, snippet_word_len):
    return 'dummy snippet'


def map_to_result_node(index_files_map, p, query):
    doc_index, bm25_score = p
    title, link = index_files_map[doc_index]
    if title == "":
        title = link
    snippet = get_snippet(None, query, 50)
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

        ranked_documents_list = list(map(map_to_node_function, ranked_documents_list))
        ranked_documents_list = ranked_documents_list[:20]

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
