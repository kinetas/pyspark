"""
02/실습코드/01의 스크립트를 고친 뒤 code-bucket에 재업로드할 때 쓰는 유틸리티.
DAG가 s3a://code-bucket/{key}를 참조하므로, 여기서 파일을 수정해도 재업로드 전까지는
실행 중인 파이프라인에 반영되지 않는다.

실행: python 03/upload_to_bucket.py
(로컬 호스트에서 실행하는 걸 전제로 MinIO는 localhost:9000 포트포워딩을 사용한다.
 파드 내부에서 쓰는 172.19.0.4는 도커 네트워크 내부 주소라 호스트에서는 안 먹는다.)
"""
import boto3

S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "code-bucket"

# bucket 안에서의 key -> 로컬 파일 경로
FILES = {
    "start.py": "02/실습코드/01/start.py",
    "02.py": "02/실습코드/01/02.py",
    "03.py": "02/실습코드/01/03.py",
    "ingest.py": "03/ingest/ingest.py",
}


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    for key, local_path in FILES.items():
        s3.upload_file(local_path, BUCKET, key)
        print(f"uploaded {local_path} -> s3a://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
