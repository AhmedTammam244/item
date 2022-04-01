import logging


class AWSUtil:
    def __init__(self, spark):
        self.logger = logging.getLogger('my-worker')
        self.logger.warning('initialize App AWS Util')
        self.spark = spark

    def read_csv_from_s3(self, bucket_name, blob_path):
        """
        Return dataframe from csv file.
        """
        file_path = f's3n://{bucket_name}/{blob_path}'
        df = self.spark.read.option("header", "true").csv(file_path)
        return df

    def df_to_redshift(self, df, jdbcURL_Writer, tempS3Dir, postactions=';'):
        """
        Return None
        """
        df.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", jdbcURL_Writer) \
            .option("dbtable", "temp_item") \
            .option("forward_spark_s3_credentials", "true") \
            .option("tempdir", tempS3Dir) \
            .option("postactions", postactions) \
            .mode("append") \
            .save()

        return True
