import tensorflow as tf
import tensorflow_hub as hub

class universal_sent_encoder(object):

    def __init__(self):
        self._uni_sent_enc_url = 'https://tfhub.dev/google/universal-sentence-encoder-large/5'
        self._uni_sent_enc = hub.load(self._uni_sent_enc_url)

    @property
    def uni_sent_enc(self):
        return self._uni_sent_enc