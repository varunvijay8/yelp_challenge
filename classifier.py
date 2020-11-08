from spark.spark_api import spark
from data.storage_factory import storage_factory
from nlp_lib.nltk_lib import nltk_lib
from nlp_lib.spacy_lib import spacy_lib
from bs4 import BeautifulSoup
from nlp_lib.universal_sentence_encoder import universal_sent_encoder
import pandas as pd
import numpy as np

DATABASE_NAME = "yelp_dataset"
TABLE_NAME = "reviews"

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
    nltk_inst = nltk_lib()
    spacy_inst = spacy_lib()
    encoder_inst = universal_sent_encoder()

    # 2) Create instance of storage class
    review_storage = storage_factory.getstorage(get_storage_type(), 'review')     # review path
    business_storage = storage_factory.getstorage(get_storage_type(), 'business') # business path

    # 3) Load review and business json into spark data frame
    review_df = spark_inst.load_data(review_storage.get_path())
    business_df = spark_inst.load_data(business_storage.get_path())

    # 4) Filter out businesses that are not restuarant 
    restaurant_business = spark_inst.drop_column(spark_inst.filter_dataframe_sql(business_df,'categories like "%Restaurant%"'), 'stars')
    merged_df = spark_inst.merge_dataframes(review_df, restaurant_business, "business_id", "inner")
    #print(merged_df.head())

    # 5) Create database and table
    spark_inst.run_sql('drop database if exists yelp_dataset cascade')
    spark_inst.run_sql('create database {0}'.format(DATABASE_NAME))
    dbs = spark_inst.run_sql('show databases')

    spark_inst.set_conf("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")  # spark configuration to create table in non-empty location
    spark_inst.create_table(merged_df, DATABASE_NAME, TABLE_NAME)
    spark_inst.run_sql('use {0}'.format(DATABASE_NAME))
    spark_inst.run_sql('REFRESH table {0}'.format(TABLE_NAME))
    tbls = spark_inst.run_sql('show tables')
    #print(tbls.toPandas())

    # 6) Filter out largest business
    review_business_postal_df = spark_inst.run_sql('''SELECT business_id, text, postal_code FROM reviews''')
    #print(review_business_postal_df.head())
    business_grouped_desc = spark_inst.order_by_count(spark_inst.group_dataframe(spark_inst.select_column(review_business_postal_df, '*'), 'business_id'))
    business_desc_review_cnt_pd = business_grouped_desc.toPandas()
    
    top_business = business_desc_review_cnt_pd.business_id[0]
    top_business_df = spark_inst.filter_dataframe(review_business_postal_df, 
                                                  review_business_postal_df.business_id, str(top_business))
    result = top_business_df
    

    # 7) Pre-process reviews and tokenize to sentences
    udf_sent_tokenize = spark_inst.udf_string_type(nltk_inst.sentence_tokenize())
    sentences = spark_inst.apply_udf(result, "text", udf_sent_tokenize, result.text)
    print(sentences[:10])
    sentences_tokenized = [sent for row in sentences for sent in row.text]
    sentence_df = pd.DataFrame(sentences_tokenized, columns=['text'])
    print(len(sentence_df.text))
    # Filter out sentences that has less than 3 words
    sentence_series = sentence_df[sentence_df.text.str.split().apply(len) > 3]
    sentence_df = pd.DataFrame(sentence_series, columns=['text'])
    # Lemmatize sentences
    lemmatized_sent_series = sentence_df.text.apply(spacy_inst.spacy_lemmatize_sentence)
    lemmatized_sent_df = pd.DataFrame(lemmatized_sent_series.to_list(), columns=['review_lemma'])
    print(lemmatized_sent_df.head())

    # 8) Encode lemmatized sentences
    grouped_df = spark_inst.group_dataframe_by_group_number(lemmatized_sent_df, np.arange(len(lemmatized_sent_df)) // 400)
    encoded_sent_np = np.empty((0,512), dtype=float)
    for n, gp in grouped_df:
        encoded_sent_np = np.append(encoded_sent_np, gp.apply(encoder_inst.uni_sent_enc).iloc[0], axis=0)
    print(encoded_sent_np[:8])
    

