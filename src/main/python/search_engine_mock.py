def node(title, link, preview):
    return {'title': title, 'link': link, 'snippet': preview}


class SearchEngine:
    def ask(self, query):
        ans = dict()
        ans['all_web'] = [
            node('First', 'google.com', 'lorem ipsum dolor sit amet'),
            node('Second', 'yandex.com', 'zhili u babusi dva veselyh gusya'),
            node('Third', 'yahoo.com', 'dirizhabl aga chita'),
            node('Fourth', 'norman.ru', 'napilasya ya pyana'),
        ]
        ans['sovf_best'] = node('why sorted array processing is faster', 'stackoverflow.com',
                                'bla bla bla too long to read bla bla bla too long to read bla bla bla too long to '
                                'read bla bla bla too long to read bla bla bla too long to read bla bla bla too long '
                                'to read bla bla bla too long to read bla bla bla too long to read bla bla bla too '
                                'long to read bla bla bla too long to read bla bla bla too long to read bla bla bla '
                                'too long to read bla bla bla too long to read bla bla bla too long to read bla bla '
                                'bla too long to read bla bla bla too long to read')
        return ans
