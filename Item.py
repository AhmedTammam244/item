import logging
from pyspark.sql.functions import from_unixtime
from AWSUtil import AWSUtil


class Item:
    def __init__(self, spark, tempS3Dir, jdbcURL_Writer):
        self.logger = logging.getLogger('my-worker')
        self.logger.warning('initialize App items')
        self.spark = spark
        self.aws_util = AWSUtil(self.spark)
        self.tempS3Dir = tempS3Dir
        self.jdbcURL_Writer = jdbcURL_Writer

    def run(self, bucket_name, blob_path, postactions=';'):
        try:
            df = self.aws_util.read_csv_from_s3(bucket_name, blob_path)
            df_property, df_available, df_categoryid = self.parsing_df(df)

            # todo create, insert into temp table
            AWSUtil(self.aws_util).df_to_redshift(df_available, self.jdbcURL_Writer, self.tempS3Dir)
            AWSUtil(self.aws_util).df_to_redshift(df_categoryid, self.jdbcURL_Writer, self.tempS3Dir)
            AWSUtil(self.aws_util).df_to_redshift(df_property, self.jdbcURL_Writer, self.tempS3Dir, postactions)

            print(f'blob {blob_path} loaded into temp table successfully')
            return True

        except Exception as e:
            print(e)
            return False

    def parsing_df(self, df):
        """
        parsing items dataframe to property, available, and category dataframe.
        """
        date_format = "yyyy-MM-dd HH:mm:ss"
        df = df.withColumn('created_at', from_unixtime(df.timestamp / 1000, date_format)).drop(*['timestamp'])

        # todo un-stem property from n35.000 to 35
        # todo deconde encoded property, value
        df_property = df.filter((df.property != "categoryid") & (df.property != "available"))
        df_property = (df_property.orderBy('timestamp')
                             .coalesce(1).dropDuplicates(subset=['itemid', 'property', 'value']))

        df_available = df.filter(df.property == "available").withColumnRenamed('value', "available").drop(
            *['property', 'value'])

        df_categoryid = df.filter(df.property == "categoryid").withColumnRenamed('value', "categoryid").drop(
            *['property', 'value'])

        return df_property, df_available, df_categoryid

    def get_values_by_key(self):
        # todo check for first items by categoryid, available if none check pervois items data, and
        #  see if this item have categoryid, available or not and update this data before insert into item table

        query = """
        """

        df = self.spark.read \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", self.jdbcURL_Writer) \
            .option("query", query) \
            .option("forward_spark_s3_credentials", "true") \
            .option("tempdir", self.tempS3Dir) \
            .load()
