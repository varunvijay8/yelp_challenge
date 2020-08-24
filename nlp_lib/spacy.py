import spacy

class spacy(object):

    def __init__(self):
        self._spacy = spacy.load('en')

    def spacy_pos_tag_to_nltk(self, sent):
        doc = self._spacy(sent)
        return [(t.text, t.tag_) for t in doc]

    