from data.storage_interface import storage
from bs4 import BeautifulSoup

class local_storage(storage):

    def __init__(self, json_file_name: str):
        '''
        Initialize the path using the text from review_json tag in the configuration
        file local_storage_config.xml
        '''
        with open("data/local_storage_config.xml") as fp:
            soup = BeautifulSoup(fp, 'xml')
            review_path = soup.find_all('{0}_json'.format(json_file_name))

        self.set_path(review_path[0].get_text())

    def set_path(self, path):
        '''
        Sets path to the json file from yelp data set
        '''
        self._path = path

    def get_path(self):
        '''
        Gets path to the json file from yelp data set
        '''
        return self._path