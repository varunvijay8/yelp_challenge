from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer

class sklearn_agglomerative_model(object):

    def __init__(self, clusters: int = 2, affinity: str ='euclidean', linkage: str ='ward'):
        self._agglomerative_model = AgglomerativeClustering(n_clusters=clusters, affinity=affinity,linkage=linkage)

    @property
    def agglomerative_model(self):
        return self._agglomerative_model

