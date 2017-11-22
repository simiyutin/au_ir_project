from project_dir import project_dir
import os, glob, json
from processer import process_text


def concat_chunks(acceptor, donor):
    for word, docs in donor.items():
        if word not in acceptor.keys():
            acceptor[word] = dict()
        for doc, num in docs.items():
            acceptor[word][doc] = num


if __name__ == '__main__':
    os.chdir(project_dir)
    all_chunks = glob.glob("bm25IndexChunk*.txt")
    index = dict()
    for ind, filename in enumerate(all_chunks):
        print('processing {} / {} chunk..'.format(ind + 1, len(all_chunks)))
        with open(project_dir + filename, 'r') as file:
            chunk = json.load(file)
        concat_chunks(index, chunk)

    print('ready to take queries')

    query = 'kotlin'
    processed_query = process_text(query)
    ranked_documents = dict()
    for term in processed_query:
        docs = index.get(term, dict())
        for doc, doc_score in docs.items():
            prev_score = ranked_documents.get(doc, 0)
            ranked_documents[doc] = prev_score + doc_score

    ranked_documents_list = ranked_documents.items()
    ranked_documents_list = sorted(ranked_documents_list, key=lambda it: it[1], reverse=True)

    with open(project_dir + 'indexFilesMap.txt') as fp:
        index_files_map = json.load(fp)

    ranked_documents_list = list(map(lambda p: (index_files_map[p[0]], p[1]), ranked_documents_list))

    print(ranked_documents_list)
