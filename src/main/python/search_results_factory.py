def node(title, link, preview):
    return {'title': title, 'link': link, 'snippet': preview}


def search_results(all_web_nodes_list, sovf_best=None):
    ans = dict()
    ans['all_web'] = all_web_nodes_list
    if sovf_best is not None:
        ans['sovf_best'] = sovf_best
    return ans
