#!/usr/bin/env python
# coding: utf-8

# In[1]:


# # [인프라] PySpark 응용 프로그램의 시작점이 되는 SparkSession 클래스를 가져옵니다.
# from pyspark.sql import SparkSession

# # [지휘관 선언] 스파크 세션을 빌더 패턴으로 생성합니다. 앱 이름을 지정하고 앱을 활성화합니다.
# # (실무 표준: 코드 내부에는 .master("k8s://...")를 적지 않아 이 코드를 YARN이나 클라우드에서도 재사용할 수 있게 만듭니다.)
# # S3 연동 패키지 다운로드를 명시한 새로운 스파크 세션 생성
# # ★ 핵심: 아래 라인을 추가하여 S3AFileSystem 클래스가 포함된 외부 패키지를 강제로 로드합니다.
# spark = SparkSession.builder \
#     .appName("K8sClusterModeJob") \
#     .master("k8s://https://k8s-cluster:6443") \
#     .config("spark.submit.deployMode", "cluster") \
#     .config("spark.kubernetes.container.image", "datamechanic/spark:3.4.1-hadoop-3.3.2-java-11-scala-2.12-latest") \
#     .config("spark.executor.instances", "2") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
#     .getOrCreate()

# # [네트워크] Spark 내부의 자바(JVM) 게이트웨이를 통해 하둡(Hadoop) 파일 시스템 설정 객체를 불러옵니다.
# hadoop_conf = spark._jsc.hadoopConfiguration()

# # [S3 설정] 가상 S3(MinIO) 컨테이너의 네트워크 주소를 하둡 시스템에 등록합니다.
# hadoop_conf.set("fs.s3a.endpoint", "http://172.19.0.3:9000")

# # [S3 설정] AWS S3의 ID와 같은 가상 Access Key를 입력합니다. (MinIO 기본값: minioadmin)
# hadoop_conf.set("fs.s3a.access.key", "minioadmin")

# # [S3 설정] AWS S3의 비밀번호와 같은 가상 Secret Key를 입력합니다. (MinIO 기본값: minioadmin)
# hadoop_conf.set("fs.s3a.secret.key", "minioadmin")

# # [S3 설정] MinIO 같은 사설 S3 호환 저장소는 도메인 방식이 아닌 '도메인/버킷명' 주소 형식을 쓰도록 강제합니다.
# hadoop_conf.set("fs.s3a.path.style.access", "true")

# # [S3 설정] s3a:// 로 시작하는 주소를 처리할 때 하둡의 S3A 파일 시스템 알고리즘 라이브러리를 사용하겠다고 선언합니다.
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# # [데이터 로드] 가상 S3의 test-bucket 버킷에서 e-commerce.csv 파일을 분산해서 읽어와 데이터프레임(df)으로 만듭니다.
# # (header=True: 첫 줄을 컬럼명으로 인식, inferSchema=True: 데이터 타입을 자동으로 추정)
# df = spark.read.csv("s3a://test-bucket/2019-Nov.csv", header=True, inferSchema=True)

# # [분산 연산] 카테고리 코드(category_code)별로 그룹화하여 데이터 개수를 세는 연산(집계)을 정의합니다.
# # (이 시점에는 연산이 실행되지 않고, 나중에 저장 명령(Action)이 떨어질 때 일꾼 팟들이 나누어 연산합니다.)
# df_result = df.groupBy("category_code").count()

# # [데이터 저장] 가상 S3 저장소의 output/cluster_mode_result 폴더에 연산 결과를 대용량 처리에 최적화된 Parquet 포맷으로 저장합니다.
# # (mode("overwrite"): 동일한 경로에 기존 데이터가 있다면 덮어쓰기 합니다.)
# df_result.write.mode("overwrite").parquet("s3a://test-bucket/output/cluster_mode_result")

# # [자원 해제] 모든 연산이 완료되었으므로 생성했던 스파크 세션을 종료하고 자원을 쿠버네티스에 반납합니다.
# spark.stop()


# In[ ]:


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
df_result = df.groupBy("category_code").count()
df_result.write.mode("overwrite").parquet("s3a://test-bucket/output/cluster_mode_result")

spark.stop()

