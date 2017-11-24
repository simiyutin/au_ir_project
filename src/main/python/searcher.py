#! /usr/bin/python3

from project_dir import project_dir
import os, glob, json
from processer import process_text


def concat_chunks(acceptor, donor):
    for word, docs in donor.items():
        if word not in acceptor.keys():
            acceptor[word] = dict()
        for doc, num in docs.items():
            acceptor[word][doc] = num


def get_ranked_docs_for_query():
    ranked_documents = dict()
    for term in processed_query:
        docs = index.get(term, dict())
        for doc, doc_score in docs.items():
            prev_score = ranked_documents.get(doc, 0)
            ranked_documents[doc] = prev_score + doc_score

    ranked_documents_list = ranked_documents.items()
    ranked_documents_list = sorted(ranked_documents_list, key=lambda it: it[1], reverse=True)
    ranked_documents_list = list(map(lambda p: (index_files_map[p[0]], p[1]), ranked_documents_list))
    ranked_documents_list = ranked_documents_list[:10]
    return ranked_documents_list


def merge_lists(list1, list2):
    return sorted(list1 + list2, key=lambda it: it[1], reverse=True)


if __name__ == '__main__':
    # web index
    all_chunks = glob.glob(project_dir + "bm25IndexChunk*.txt")
    index = dict()
    for ind, filepath in enumerate(all_chunks):
        print('processing {} / {} chunk..'.format(ind + 1, len(all_chunks)))
        with open(filepath, 'r') as file:
            chunk = json.load(file)
        concat_chunks(index, chunk)

    with open(project_dir + 'indexFilesMap.txt') as fp:
        index_files_map = json.load(fp)

    # sovf index
    all_chunks_stack = glob.glob(project_dir + "bm25IndexStackChunk*.txt")
    index_stack = dict()
    for ind, filepath in enumerate(all_chunks_stack):
        print('processing {} / {} sovf chunk..'.format(ind + 1, len(all_chunks_stack)))
        with open(filepath, 'r') as file:
            chunk = json.load(file)
        concat_chunks(index_stack, chunk)

    with open(project_dir + 'indexPostsMap.txt') as fp:
        index_files_map_stack = json.load(fp)

    print('ready to take queries')
    query = 'binary search'
    while True:
        processed_query = process_text(query)

        ranked_documents_list = get_ranked_docs_for_query(index, query)
        ranked_documents_list_stack = get_ranked_docs_for_query(index_stack, query)
        if ranked_documents_list_stack[0][1] > 20:
            popup_card = ranked_documents_list_stack[0]  # todo show in interface
            ranked_documents_list_stack = ranked_documents_list_stack[:-1]

        ranked_documents_list = merge_lists(ranked_documents_list, ranked_documents_list_stack)

        print('query: {}'.format(query))
        print('documents: {}'.format(ranked_documents_list))
        query = input('enter new query:')
