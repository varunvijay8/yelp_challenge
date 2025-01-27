import pyspark
from pyspark.sql.functions import col, desc, udf
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType

class spark(object):

    def __init__(self):
        self._spark = pyspark.sql.SparkSession.builder.appName('yelp_eda').getOrCreate()

    def spark_session(self):
        '''
        Accessor to spark sesssion
        '''
        return self._spark

    def set_conf(self, conf_name: str, conf_val: str):
        '''
        Set spark configuration
        '''
        self._spark.conf.set(conf_name, conf_val)

    def load_data(self, path: str):
        '''
        Returns spark data frame
        @param path     path to the yelp review json file

        @return     returns spark dataframe
        '''
        return self._spark.read.format('json').option("inferSchema", True).load(path)
    
    def select_column(self, input_df, columns: str):
        return input_df.select(columns)

    def filter_dataframe_sql(self, input_df, filter: str):
        '''
        Returns spark data frame with rows matching filter criteria
        @param input_df input spark data frame
        @param filter   filter string      
        '''
        return input_df.where(filter)

    def filter_dataframe(self, input_df, input_col, filter: str):
        '''
        Returns spark data frame with rows matching filter criteria
        @param input_df input spark data frame
        @param input_col input key column to compare with filter
        @param filter   filter string      
        '''
        return input_df.where(input_col == filter)

    def drop_column(self, input_df, column: str):
        '''
        Returns spark data frame with column dropped 
        @param input_df input spark data frame
        @param column   column to be dropped
        '''
        return input_df.drop(column)

    def merge_dataframes(self, left_df, right_df, on: str, how: str):
        '''
        Returns spark dataframe after merging left with right
        @param on   column to merge on
        @param how  type of join inner, outer etc
        '''
        return left_df.join(right_df, on=on, how=how)

    def run_sql(self, statement: str):
        '''
        Helper to issue sql queries to spark session
        '''
        try:
            result = self._spark.sql(statement)
        except Exception as e:
            print(e)
            return
        return result

    def dataframe_to_pandas(self, input_df):
        '''
        Convert spark dataframe to pandas dataframe
        @return     Pandas dataframe version of input spark dataframe
        '''
        return input_df.toPandas()

    def group_dataframe(self, input_df, group_by_col: str):
        '''
        Group rows based on unique values in input column
        @param input_df     input spark dataframe
        @param group_by_col column to use for grouping
        @return grouped dataframe
        '''
        return input_df.groupby(group_by_col)

    def group_dataframe_by_group_number(self, input_df, group_by_number: list):
        '''
        Group rows based on group numbers
        @param input_df         input spark dataframe
        @param group_by_number  list of numbers each row gets grouped to
        @return grouped dataframe
        '''
        return input_df.groupby(group_by_number)
    
    def create_table(self, input_df, database: str, tablename: str):
        '''
        Create table in input database
        '''
        input_df.write.mode('overwrite').format("parquet").saveAsTable("{0}.{1}".format(database, tablename))

    def order_by_count(self, grouped_df):
        '''
        Order unique rows from group_dataframe output in descending order
        @param grouped_df   output from group_dataframe
        @return unique groups in descending order
        '''
        return grouped_df.count().orderBy(desc('count'))

    def udf_string_type(self, input_function):
        '''
        Convert input string porcessing function to UDF
        '''
        return udf(input_function, ArrayType(StringType())).asNondeterministic()

    def apply_udf(self, input_df, new_col: str, input_udf, input_col):
        return input_df.withColumn(new_col, input_udf(input_col)).collect()
        



