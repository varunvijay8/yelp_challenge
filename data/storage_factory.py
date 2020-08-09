from data.gcs_storage_api import gcs_storage
from data.local_storage_api import local_storage

class storage_factory(object):

    @staticmethod
    def getstorage(storage_name):
        if storage_name == 'gcs':
            return gcs_storage()
        elif storage_name == 'local':
            return local_storage()
        else:
            assert 0, 'Could not find storage "%s"' %storage_name