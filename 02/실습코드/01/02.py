#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("K8sClusterModeJob").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://172.19.0.4:9000")
hadoop_conf.set("fs.s3a.access.key","minioadmin")
hadoop_conf.set("fs.s3a.secret.key","minioadmin")
hadoop_conf.set("fs.s3a.path.style.access","true")
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.parquet("s3a://test-bucket/bronze/cluster_mode_result")
pid_code_map = df.filter(sf.col('category_code').isNotNull()) \
    .select('product_id', 'category_code') \
    .dropDuplicates(['product_id']) \
    .withColumnRenamed('category_code', 'code_by_pid')

pid_brand_map = df.filter(sf.col('brand').isNotNull()) \
    .select('product_id', 'brand') \
    .dropDuplicates(['product_id']) \
    .withColumnRenamed('brand', 'brand_by_pid')

cid_map = df.filter(sf.col('category_code').isNotNull()) \
    .select('category_id', 'category_code') \
    .dropDuplicates(['category_id']) \
    .withColumnRenamed('category_code', 'code_by_cid')

df_join_temp = df.join(sf.broadcast(pid_code_map), on='product_id', how='left') \
       .join(sf.broadcast(pid_brand_map), on='product_id', how='left') \
       .join(sf.broadcast(cid_map), on='category_id', how='left')

df_join = df_join_temp.withColumn('category_code',sf.coalesce(sf.col('category_code'),sf.col('code_by_cid'),sf.col('code_by_pid'))) \
                        .withColumn('brand',sf.coalesce(sf.col('brand'),sf.col('brand_by_pid'))) \
                        .drop('code_by_cid','code_by_pid','brand_by_pid')

refined_df = df.filter(df['category_code'].isNotNull() & df['brand'].isNotNull())
refined_df = refined_df.drop('product_id','category_id','user_id','user_session')
refined_df = refined_df.withColumn("event_date", sf.to_date(sf.col("event_time")))

refined_df = df_join.filter(df_join['category_code'].isNotNull() & df_join['brand'].isNotNull())
refined_df = refined_df.drop('product_id','category_id','user_id','user_session')
refined_df = refined_df.withColumn("event_date", sf.to_date(sf.col("event_time")))

refined_df.repartition('category_code').write.mode('Overwrite').partitionBy('category_code').parquet("s3a://test-bucket/silver/ecommerce_refined")


# In[ ]:




