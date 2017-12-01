from search_results_factory import node, search_results


class SearchEngine:
    def ask(self, query):
        all_web = [
            node('First', 'google.com', 'lorem ipsum dolor sit amet'),
            node('Second', 'yandex.com', 'zhili u babusi dva veselyh gusya'),
            node('Third', 'yahoo.com', 'dirizhabl aga chita'),
            node('Fourth', 'norman.ru', 'napilasya ya pyana'),
        ]
        sovf_best = node('why sorted array processing is faster', 'stackoverflow.com',
                         'bla bla bla too long to read bla bla bla too long to read bla bla bla too long to '
                         'read bla bla bla too long to read bla bla bla too long to read bla bla bla too long '
                         'to read bla bla bla too long to read bla bla bla too long to read bla bla bla too '
                         'long to read bla bla bla too long to read bla bla bla too long to read bla bla bla '
                         'too long to read bla bla bla too long to read bla bla bla too long to read bla bla '
                         'bla too long to read bla bla bla too long to read')

        return search_results(all_web, sovf_best)
