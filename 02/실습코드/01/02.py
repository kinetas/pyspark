#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("K8sClusterModeJob").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://172.19.0.3:9000")
hadoop_conf.set("fs.s3a.access.key","minioadmin")
hadoop_conf.set("fs.s3a.secret.key","minioadmin")
hadoop_conf.set("fs.s3a.path.style.access","true")
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.csv("s3a://test-bucket/2019-Nov.csv",header=True, inferSchema=True)
mapping_df = df.filter(sf.col('category_code').isNotNull()) \
                    .select('product_id','category_id','category_code') \
                    .distinct() \
                    .withColumnRenamed('product_id','pid')\
                    .withColumnRenamed('category_id','cid')\
                    .withColumnRenamed('category_code','code')

join_condition = (df['product_id'] == mapping_df['pid']) | (df['category_id'] == mapping_df['cid'])

df = df.join(sf.broadcast(mapping_df), on=join_condition,how='left')\
        .withColumn('category_code', sf.coalesce(sf.col('category_code'),sf.col('code')))\
        .drop('pid','cid','code')

mapping_b_df = df.filter(sf.col('brand').isNotNull())\
                    .select('product_id','brand')\
                    .distinct()\
                    .withColumnRenamed('brand','b')

df = df.join(sf.broadcast(mapping_b_df), on='product_id',how='left')\
        .withColumn('brand',sf.coalesce(sf.col('brand'),sf.col('b')))\
        .drop('b')

refined_df = df.filter(df['category_code'].isNotNull() & df['brand'].isNotNull())
refined_df = refined_df.drop('product_id','category_id','user_id','user_session')
refined_df = refined_df.withColumn("event_date", sf.to_date(sf.col("event_time")))

refined_df.write.mode('overwrite').partitionBy('category_code').parquet("s3a://test-bucket/silver/ecommerce_refined")


# In[ ]:




