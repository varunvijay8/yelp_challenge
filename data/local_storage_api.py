from data.storage_interface import storage
from bs4 import BeautifulSoup

class local_storage(storage):

    def __init__(self):
        '''
        Initialize the path using the text from review_json tag in the configuration
        file local_storage_config.xml
        '''
        with open("data/local_storage_config.xml") as fp:
            soup = BeautifulSoup(fp, 'xml')
            review_path = soup.find_all('review_json')

        self.set_path(review_path[0].get_text())

    def set_path(self, path):
        self._path = path

    def get_path(self):
        return self._path