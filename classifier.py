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
    spark_inst = spark()

    # 2) Create instance of storage class
    review_storage = storage_factory.getstorage(get_storage_type(), 'review')     # review path
    business_storage = storage_factory.getstorage(get_storage_type(), 'business') # business path

    # 3) Load review and business json into spark data frame
    review_df = spark_inst.load_data(review_storage.get_path())
    business_df = spark_inst.load_data(business_storage.get_path())

    # 4) Filter out businesses that are not restuarant 
    restaurant_business = spark_inst.drop_column(spark_inst.filter_dataframe(business_df,'categories like "%Restaurant%"'), 'stars')
    merged_df = spark_inst.merge_dataframes(review_df, restaurant_business, "business_id", "inner")
    
    # 5) Create database
    spark_inst.run_sql('drop database if exists yelp_dataset cascade')
    spark_inst.run_sql('create database yelp_dataset')
    dbs = spark_inst.run_sql('show databases')
    print(dbs.toPandas())

