from data.storage_factory import storage_factory

if __name__ == "__main__":
    storage = storage_factory()
    gcs = storage_factory.getstorage('gcs')