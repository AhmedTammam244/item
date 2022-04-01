# Revision Task

### Description:
- my solution is serverless and cloud-based (AWS or GCP).
I don't use any servers.
- "architecture diagram.drawio.html" file attached with architecture in GCP, AWS.
task separated into three pipelines:
    1. category pipeline.
    2. event pipeline.
    3. item pipeline.
    

- for category and event pipelines: we have a small amount of data 
  so all we need to do is trigger on Data Lake(s3) blob using AWS Lambda that will take these files and load them into the Data Warehouse(RedShift).


- but why didn't did that with item files?
 Item files larger than category, event data,
and AWS lambda will take time to do some transformation over this data, loading into redshift.
AWS lambda max timeout is 15min, and this job will take more than 20min.


- by using AWS EMR to run apache spark over AWS: it takes less time than AWS lambda will take.


### Deployment Variables 
Each function need to have a separate deployment environment with the 
variables below

| VARIABLE NAME                 | DESCRIPTION                      |
|-------------------------------|----------------------------------|
| BUCKET_NAME                   | aws bucket                       |
| aws_access_key                | Access Key of aws account        |
| aws_secret_access_key         | Secret Access Key of aws account |
| REGION                        | eu-west-1                        |


---
## RUN job from  Spark-submit

```
spark-submit                                                                                  \
    --jars RedshiftJDBC41-1.2.12.1017.jar                                 \  
    --packages org.apache.hadoop:hadoop-aws:2.7.1,com.databricks:spark-redshift_2.11:2.0.1      \
    main.py
```

---
## RUN job from pyspark Locally

```
pyspark                                                                  \
--jars RedshiftJDBC41-1.2.12.1017.jar                  \     
--packages com.databricks:spark-redshift_2.11:2.0.1,org.apache.hadoop:hadoop-aws:2.7.1

```
---

```
Note: Redshift JDBC jar file required to run this project.
```