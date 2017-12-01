def node(title, link, snippet, type):
    return {'title': title, 'link': link, 'snippet': snippet, 'type': type}


def node_sovf(title, link, question, answer, tags):
    return {'title': title, 'link': link, 'question': question, 'answer': answer, 'tags': tags}


def search_results(all_web_nodes_list, sovf_best=None):
    ans = dict()
    ans['all_web'] = all_web_nodes_list
    if sovf_best is not None:
        ans['sovf_best'] = sovf_best
    return ans
