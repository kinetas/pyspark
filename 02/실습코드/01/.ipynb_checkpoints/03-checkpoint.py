#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("K8sStatsJob").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://172.19.0.3:9000")
hadoop_conf.set("fs.s3a.access.key","minioadmin")
hadoop_conf.set("fs.s3a.secret.key","minioadmin")
hadoop_conf.set("fs.s3a.path.style.access","true")
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.parquet("s3a://test-bucket/silver/ecommerce_refined")

# 1. 이벤트 퍼널 (view -> cart -> purchase 전환율)
funnel_df = df.groupBy('event_type').count()
funnel_df.write.mode('overwrite').parquet("s3a://test-bucket/gold/event_funnel")

# 2. 카테고리 / 브랜드별 매출 Top N (purchase 이벤트 기준)
purchase_df = df.filter(sf.col('event_type') == 'purchase')

category_revenue = purchase_df.groupBy('category_code')     .agg(
        sf.sum('price').alias('revenue'),
        sf.count('*').alias('purchase_count')
    )     .orderBy(sf.desc('revenue'))
category_revenue.write.mode('overwrite').parquet("s3a://test-bucket/gold/category_revenue")

brand_revenue = purchase_df.groupBy('brand')     .agg(
        sf.sum('price').alias('revenue'),
        sf.count('*').alias('purchase_count')
    )     .orderBy(sf.desc('revenue'))
brand_revenue.write.mode('overwrite').parquet("s3a://test-bucket/gold/brand_revenue")

# 3. 일별(event_date) 추이
daily_events = df.groupBy('event_date', 'event_type')     .count()     .orderBy('event_date', 'event_type')
daily_events.write.mode('overwrite').parquet("s3a://test-bucket/gold/daily_events")

daily_purchase = purchase_df.groupBy('event_date')     .agg(
        sf.count('*').alias('purchase_count'),
        sf.sum('price').alias('daily_revenue')
    )     .orderBy('event_date')
daily_purchase.write.mode('overwrite').parquet("s3a://test-bucket/gold/daily_purchase")

# 4. 가격 분포 / 기술통계
price_stats_df = df.select('price').describe()
price_stats_df.write.mode('overwrite').parquet("s3a://test-bucket/gold/price_stats")

price_percentiles_df = df.select(
    sf.expr('percentile_approx(price, 0.25)').alias('p25'),
    sf.expr('percentile_approx(price, 0.5)').alias('median'),
    sf.expr('percentile_approx(price, 0.75)').alias('p75'),
    sf.expr('percentile_approx(price, 0.95)').alias('p95'),
)
price_percentiles_df.write.mode('overwrite').parquet("s3a://test-bucket/gold/price_percentiles")

purchase_price_stats_df = purchase_df.select('price').describe()
purchase_price_stats_df.write.mode('overwrite').parquet("s3a://test-bucket/gold/purchase_price_stats")
