import spacy

class spacy_lib(object):

    def __init__(self):
        # Download package by using 'spacy download en'
        self._spacy = spacy.load('en', disable=['parser', 'ner'])

    def spacy_pos_tag_to_nltk(self, sent):
        doc = self._spacy(sent)
        return [(t.text, t.tag_) for t in doc]

    def spacy_lemmatize_sentence(self, sentence: str):
        sentence_lemma = self._spacy(sentence)
        return " ".join([token.text.lower() if token.lemma_ == '-PRON-' else token.lemma_ for token in sentence_lemma])

    