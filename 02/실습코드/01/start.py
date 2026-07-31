# job.py (실무 표준 배포용 파일)
from pyspark.sql import SparkSession

# 중요: 파일 내부에는 master나 deployMode 설정을 넣지 않고 공백으로 둡니다. (제출 명령어가 주입함)
spark = SparkSession.builder.appName("K8sClusterModeJob").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://172.19.0.3:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df = spark.read.csv("s3a://test-bucket/2019-Nov.csv", header=True, inferSchema=True)
df.write.mode("overwrite").parquet("s3a://test-bucket/bronze/cluster_mode_result")

spark.stop()