import nltk
from nltk import word_tokenize
from nltk import sent_tokenize
from nltk import pos_tag, pos_tag_sents
from nltk import RegexpParser
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

class nltk(object):

    def __init__(self):
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        nltk.download('maxent_ne_chunker')
        nltk.download('words')
        nltk.download('stopwords')
        nltk.download('wordnet')
        nltk.download('brown')