from spark.spark_api import spark
from data.storage_factory import storage_factory
from bs4 import BeautifulSoup

def get_storage_type():
    '''
    Read the global configuration file config.xml to get the storage type
    '''
    with open('config.xml') as fp:
        soup = BeautifulSoup(fp, 'xml')
        storage_config = soup.find_all('storage_type')

    return storage_config[0].get_text()

if __name__ == "__main__":

    # 1) Create spark session
    s = spark()

    # 2) Create instance of storage class
    storage = storage_factory.getstorage(get_storage_type())
