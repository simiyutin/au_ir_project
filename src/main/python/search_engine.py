#! /usr/bin/python3

from project_dir import project_dir

import os, glob, json
import pymssql

from os import getenv
from processer import process_text
from search_results_factory import node, node_sovf, search_results
from bs4 import BeautifulSoup


def concat_chunks(acceptor, donor, type):
    # type == 'c' if docs are crawled, 's' if they are from stackoverflow
    for word, docs in donor.items():
        if word not in acceptor.keys():
            acceptor[word] = dict()
        for doc, num in docs.items():
            acceptor[word][type + str(doc)] = num


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


def map_to_result_node(index_files_map, index_posts_map, p, query, cursor):
    doc_index, bm25_score = p
    doc_type = doc_index[0]
    doc_index = doc_index[1:]
    if doc_type == 'c':
        title, link, original_name = index_files_map[doc_index]
        snippet = get_snippet(project_dir + '/crawled/' + original_name, query, 50)
        return node(title, link, snippet, doc_type)
    else:
        original_id = index_posts_map[doc_index]

        cursor.execute(f"""
        select
            q.Title,
            q.Body
        from Posts q
        where q.Id = {original_id}
        """)

        (title, query_body) = cursor.fetchone()

        ## todo: snippet !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        return node(title, f"https://stackoverflow.com/questions/{original_id}", query_body[:50], doc_type)


class SearchEngine:
    def __init__(self):
        crawled_chunks = glob.glob(project_dir + "bm25IndexChunk*.txt")
        stack_chunks = glob.glob(project_dir + "bm25StackIndexChunk*.txt")
        self.index = dict()
        for ind, filepath in enumerate(crawled_chunks):
            print('processing {} / {} of crawled chunks..'.format(ind + 1, len(crawled_chunks)))
            with open(filepath, 'r') as file:
                chunk = json.load(file)
            concat_chunks(self.index, chunk, 'c')
        for ind, filepath in enumerate(stack_chunks):
            print('processing {} / {} of stack chunks..'.format(ind + 1, len(stack_chunks)))
            with open(filepath, 'r') as file:
                chunk = json.load(file)
            concat_chunks(self.index, chunk, 's')

        with open(project_dir + 'indexFilesMap.txt') as fp:
            self.index_files_map = json.load(fp)
        with open(project_dir + 'indexPostsMap.txt') as fp:
            self.index_posts_map = json.load(fp)

        print('ready to take queries')

    def ask(self, query, server, user, password):
        processed_query = process_text(query)
        ranked_documents = dict()
        for term in processed_query:
            docs = self.index.get(term, dict())
            for doc, doc_score in docs.items():
                prev_score = ranked_documents.get(doc, 0)
                ranked_documents[doc] = prev_score + doc_score

        ranked_documents_list = ranked_documents.items()
        ranked_documents_list = sorted(ranked_documents_list, key=lambda it: it[1], reverse=True)
        top_doc, top_rank = ranked_documents_list[0]

        connection = pymssql.connect(server, user, password, "stackDB")
        cursor = connection.cursor()

        def map_to_node_function(p):
            return map_to_result_node(self.index_files_map, self.index_posts_map, p, processed_query, cursor)

        ranked_documents_list = ranked_documents_list[:20]
        ranked_documents_list = list(map(map_to_node_function, ranked_documents_list))

        # stack overflow result sample
        sovf_best = None
        #     node_sovf(
        #     title='Does Python have a string \'contains\' substring method?',
        #     link='https://stackoverflow.com/questions/3437059/does-python-have-a-string-contains-substring-method',
        #     question='I\'m looking for a string.contains or string.indexof method in Python. I want to do: if not '
        #              'somestring.contains("blah"): continue',
        #     answer='You can use the in operator: if "blah" not in somestring: continue',
        #     tags=['python', 'string', 'substring', 'contains']
        # )

        if top_doc[0] == 's':
            if top_rank > 6. * len(processed_query):
                cursor = connection.cursor()
                original_id = self.index_posts_map[top_doc[1:]]
                cursor.execute(f"""
                select
                    q.Title,
                    q.Body,
                    q.Tags,
                    q.AcceptedAnswerId
                from Posts q
                where q.Id = {original_id}
                """)

                (title, query_body, tags, acc_id) = cursor.fetchone()

                cursor.execute(f"""
                select
                    q.Body
                from Posts q
                where q.Id = {acc_id}
                """)

                answer = cursor.fetchone()
                sovf_best = node_sovf(title, f"https://stackoverflow.com/questions/{original_id}", query_body, answer, tags)

        connection.close()

        results = search_results(ranked_documents_list, sovf_best)
        return results


if __name__ == '__main__':
    server = getenv("MS_SERVER")
    user = getenv("MS_USERNAME")
    password = getenv("MS_PASSWORD")

    searchEngine = SearchEngine()

    query = 'binary search'
    while True:
        results = searchEngine.ask(query, server, user, password)

        print('query: {}'.format(query))
        print('documents: {}'.format(results))
        query = input('enter new query:')
