"""
Kaggle 데이터셋의 파일 하나를 받아서 MinIO(S3 호환)에 올린다.
이 스크립트는 Airflow 프로세스 밖, KubernetesPodOperator가 만드는 별도 팟 안에서 돈다.
그래서 Airflow의 Hook/Connection이 아니라 표준 라이브러리(kaggle CLI, boto3)만 사용한다.

필요한 환경변수:
  KAGGLE_USERNAME, KAGGLE_KEY   - kaggle CLI가 자동으로 읽음 (K8s Secret으로 주입됨)
  KAGGLE_DATASET                - 예: mkechinov/ecommerce-behavior-data-from-multi-category-store
  KAGGLE_FILE                   - 데이터셋 안에서 받을 파일명, 예: 2019-Oct.csv
  S3A_ENDPOINT                  - 예: http://172.19.0.3:9000
  MINIO_ACCESS_KEY, MINIO_SECRET_KEY  (K8s Secret으로 주입됨)
  MINIO_BUCKET                  - 기본값 test-bucket
"""
import os
import subprocess
import sys
import zipfile

import boto3


def download_from_kaggle(dataset: str, file_name: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    # 주의: --unzip은 컨테이너 안에 시스템 unzip 바이너리가 있어야 동작하는데
    # python:3.11-slim(Debian slim)엔 없어서 "성공"이라고 하고(exit 0) 조용히 압축을 안 풂.
    # 그래서 --unzip을 빼고 zip으로 받은 뒤, 표준 라이브러리 zipfile로 직접 푼다(외부 바이너리 의존성 없음).
    cmd = [
        "kaggle", "datasets", "download",
        "-d", dataset,
        "-f", file_name,
        "-p", dest_dir,
        "--force",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"kaggle download failed (exit {result.returncode})")

    zip_path = os.path.join(dest_dir, f"{file_name}.zip")
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"expected zip not found after download: {zip_path}")

    print(f"extracting {zip_path}")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)
    os.remove(zip_path)

    local_path = os.path.join(dest_dir, file_name)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"expected file not found after extract: {local_path}")
    return local_path


def upload_to_minio(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3A_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    print(f"uploading {local_path} -> s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key)
    print("upload done")


def main() -> None:
    dataset = os.environ["KAGGLE_DATASET"]
    file_name = os.environ["KAGGLE_FILE"]
    bucket = os.environ.get("MINIO_BUCKET", "test-bucket")
    dest_dir = "/tmp/kaggle"

    local_path = download_from_kaggle(dataset, file_name, dest_dir)
    upload_to_minio(local_path, bucket, file_name)
    os.remove(local_path)


if __name__ == "__main__":
    main()
