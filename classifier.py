from spark.spark_api import spark
from data.storage_factory import storage_factory

if __name__ == "__main__":

    # 1) Create spark session
    s = spark()

    # 2) Create instance of storage class
    storage = storage_factory.getstorage('local')
