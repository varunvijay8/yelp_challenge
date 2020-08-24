import nltk
from nltk import word_tokenize
from nltk import sent_tokenize
from nltk import pos_tag, pos_tag_sents
from nltk import RegexpParser
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from spacy import spacy

class nltk(object):

    def __init__(self):
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        nltk.download('maxent_ne_chunker')
        nltk.download('words')
        nltk.download('stopwords')
        nltk.download('wordnet')
        nltk.download('brown')
        self._spacy_obj = spacy()

    def chunk_nouns_3(self, sent):
        lst = []
        token_tag = self._spacy_obj.spacy_pos_tag_to_nltk(sent)
        grammar = r"""NP: {(<JJ><CC>)?<JJ>?<NN.*>+}"""
        noun_parser = RegexpParser(grammar)
        parsed_output = noun_parser.parse(token_tag)
        for subtree in parsed_output.subtrees(filter=lambda t: t.label() == 'NP'):
            lst.append(' '.join(word for (word, pos) in subtree.leaves()))
        return lst

    
