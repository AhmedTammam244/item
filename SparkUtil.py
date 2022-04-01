import logging
region = "eu-west-1"
aws_access_key = "aws_access_key"
aws_secret_access_key = "aws_secret_access_key"


class SparkUtil:
    def __init__(self, sc):
        self.logger = logging.getLogger('my-worker')
        self.logger.warning('initialize App Spark Util')
        self.sc = sc

    def set_hadoop_config(self):
        """
        set hadoop Configuration for spark
        """
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", aws_access_key)
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", aws_secret_access_key)

        self.sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        self.sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                          "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"{region}.amazonaws.com")
