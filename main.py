from SparkUtil import SparkUtil
from Item import Item
from pyspark import SparkConf, SparkContext, SQLContext
from . import user, password, host, port, database_name, bucket_name

conf = SparkConf().setAppName("item").setMaster("yarn")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)
SparkUtil(sc).set_hadoop_config()

# todo read all files from item subdirectory directly and loop on it
blob_path = 'item/item_properties_part1.csv'
blob_path2 = 'item/item_properties_part2.csv'

tempS3Dir = "s3://item-data/temp/"
jdbcURL_Writer = f"jdbc:redshift://{host}:{port}/{database_name}?user={user}&password={password}"
item = Item(spark, tempS3Dir, jdbcURL_Writer)

result1 = item.run(bucket_name, blob_path)

query = """insert into item ( select created_at, itemid, property, value,  categoryid, available from ( select created_at, itemid, property, value, lag(categoryid, 1 IGNORE NULLS) over (partition by itemid order by created_at) as categoryid, lag(available, 1 IGNORE NULLS) over (partition by itemid order by created_at)  as available from temp_item order by created_at, itemid) where (property is not null and value is not null));"""
result2 = item.run(bucket_name, blob_path2, query)


if result1 and result2:
    print('success , file will move to s3://revision-data/items/success/2022/04/02/')
    # todo move file to success path "items/success/2022/04/02/"
    pass

else:
    print("error, file will move to s3://revision-data/items/error/2022/04/02/")
    # todo move file to error path "items/error/2022/04/02/"
    pass
