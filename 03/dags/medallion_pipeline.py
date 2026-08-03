"""
02(Kubernetes 위 Spark) 파이프라인을 Airflow로 오케스트레이션한다.

지금까지 손으로 하던 순서를 그대로 자동화하는 것뿐이다:
  kubectl run(제출자 팟) -> spark-submit --deploy-mode cluster -> driver 팟 생성
  bronze(start.py) 성공 확인 -> silver(02.py) 제출 -> 성공 확인 -> gold(03.py) 제출

KubernetesPodOperator가 "제출자 팟"을 만들고 그 안에서 spark-submit이 실제 driver 팟을 다시 만드는 구조는
02/start.md의 "시도 7(최종 성공)"과 완전히 동일하다. --conf 값들도 시도7 + 11-2(리소스 제한)에서
검증 끝난 값을 그대로 재사용한다. 코드(start.py/02.py/03.py)는 이미 code-bucket에 올라가 있으므로
여기서는 새로 작성하지 않고 s3a 경로만 참조한다.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

# docker-compose.yml의 kubeconfig-prep 컨테이너가 만들어 둔 kubeconfig 경로
KUBE_CONFIG_PATH = "/opt/airflow/kube/config"
NAMESPACE = "default"
SPARK_IMAGE = "apache/spark:3.4.1"

# MinIO 컨테이너(k8s-s3-minio) IP. 02/명령어.md 참고 — 컨테이너 재생성 시 바뀔 수 있으므로
# `docker inspect k8s-s3-minio --format '{{json .NetworkSettings.Networks}}'`로 재확인 후 여기만 고치면 됨.
S3A_ENDPOINT = "http://172.19.0.3:9000"

# 자격증명은 DAG 코드/이미지에 안 넣고 K8s Secret에서만 가져온다 (kubectl create secret으로 미리 생성해둠).
KAGGLE_SECRETS = [
    Secret("env", "KAGGLE_USERNAME", "kaggle-credentials", "username"),
    Secret("env", "KAGGLE_KEY", "kaggle-credentials", "key"),
]
MINIO_SECRETS = [
    Secret("env", "MINIO_ACCESS_KEY", "minio-credentials", "access-key"),
    Secret("env", "MINIO_SECRET_KEY", "minio-credentials", "secret-key"),
]

# ingest.py는 이미지에 굽지 않고 start.py/02.py/03.py와 같은 방식으로 code-bucket에 올려둔다.
# 팟은 뜨자마자 boto3로 그 파일 하나만 받아서 바로 실행한다(부트스트랩만 인라인, 실제 로직은 진짜 .py 파일).
INGEST_BOOTSTRAP = """
set -euo pipefail
pip install --quiet --no-cache-dir boto3 kaggle
python3 -c "
import boto3, os
s3 = boto3.client('s3', endpoint_url=os.environ['S3A_ENDPOINT'],
                   aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
                   aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
s3.download_file('code-bucket', 'ingest.py', '/tmp/ingest.py')
"
python3 /tmp/ingest.py
"""

COMMON_CONF = [
    "--conf", f"spark.kubernetes.container.image={SPARK_IMAGE}",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.dynamicAllocation.enabled=true",
    "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=true",
    "--conf", "spark.dynamicAllocation.minExecutors=1",
    "--conf", "spark.dynamicAllocation.maxExecutors=2",
    "--conf", "spark.dynamicAllocation.initialExecutors=1",
    "--conf", "spark.driver.memory=1g",
    "--conf", "spark.executor.memory=1g",
    "--conf", "spark.executor.memoryOverhead=512m",
    "--conf", "spark.eventLog.enabled=true",
    "--conf", "spark.eventLog.dir=s3a://code-bucket/spark-events",
    "--conf", "spark.jars.ivy=/tmp/.ivy",
    "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "--conf", f"spark.hadoop.fs.s3a.endpoint={S3A_ENDPOINT}",
    "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
    "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
    "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
]


def spark_submit_args(job_name: str, script_path: str) -> list[str]:
    return [
        "--master", "k8s://https://kubernetes.default.svc",
        "--deploy-mode", "cluster",
        "--name", job_name,
        *COMMON_CONF,
        script_path,
    ]


def make_task(task_id: str, job_name: str, script_path: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=job_name,                  # 팟 이름 접두어. Airflow가 실행마다 랜덤 suffix를 붙여 유일하게 만들어줌
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=spark_submit_args(job_name, script_path),
        service_account_name="spark",   # 제출자 팟 자신이 driver 팟을 생성할 권한 (02/start.md 시도6 RBAC)
        config_file=KUBE_CONFIG_PATH,
        in_cluster=False,
        get_logs=True,                  # 제출자 팟 로그를 Airflow 태스크 로그로 스트리밍 (매번 kubectl logs 안 쳐도 됨)
        is_delete_operator_pod=True,    # 끝나면 제출자 팟 자동 삭제 (매번 손으로 kubectl delete pod 안 해도 됨)
        startup_timeout_seconds=300,
    )


with DAG(
    dag_id="medallion_pipeline",
    description="Ingest(Kaggle) -> Bronze -> Silver -> Gold (02 폴더 spark-submit 파이프라인을 그대로 자동화)",
    start_date=datetime(2026, 8, 1),
    schedule=None,          # 학습 중이라 수동 트리거. 매일 자동 실행하려면 "@daily"로 바꾸면 됨
    catchup=False,
    tags=["spark", "k8s", "medallion"],
    params={"kaggle_file": "2019-Oct.csv"},  # Trigger DAG w/ config로 다른 달 파일도 지정 가능
) as dag:

    ingest_task = KubernetesPodOperator(
        task_id="ingest_raw_data",
        name="ingest-kaggle-raw",
        namespace=NAMESPACE,
        image="python:3.11-slim",
        cmds=["bash", "-c"],
        arguments=[INGEST_BOOTSTRAP],
        env_vars={
            "S3A_ENDPOINT": S3A_ENDPOINT,
            "KAGGLE_DATASET": "mkechinov/ecommerce-behavior-data-from-multi-category-store",
            "KAGGLE_FILE": "{{ params.kaggle_file }}",
        },
        secrets=KAGGLE_SECRETS + MINIO_SECRETS,
        config_file=KUBE_CONFIG_PATH,
        in_cluster=False,
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
        container_resources={
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"memory": "1Gi"},
        },
    )

    bronze_task = make_task(
        task_id="bronze_to_parquet",
        job_name="bronze-parquet-job",
        script_path="s3a://code-bucket/start.py",
    )

    silver_task = make_task(
        task_id="silver_refine",
        job_name="silver-refine-job",
        script_path="s3a://code-bucket/02.py",
    )

    gold_task = make_task(
        task_id="gold_stats",
        job_name="gold-stats-job",
        script_path="s3a://code-bucket/03.py",
    )

    ingest_task >> bronze_task >> silver_task >> gold_task
