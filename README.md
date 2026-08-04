# pyspark

PySpark 학습용 실습 저장소. Docker 기반 Jupyter + Spark 환경에서 RDD/DataFrame/Spark SQL을 단계별로 실습합니다.

## 구성

| 폴더 | 환경 | 설명 |
| --- | --- | --- |
| [`00/`](00) | 싱글 노드 | Jupyter 한 컨테이너에서 Spark를 로컬(`local`) 모드로 실행. RDD/DataFrame 기본기 실습 |
| [`01/`](01) | 멀티 노드 클러스터 | Jupyter(마스터) + Spark 워커 2대로 구성된 클러스터. 실제 클러스터 연결·분산 처리 실습 |
| [`02/`](02) | Kubernetes(k3s) 클러스터 모드 | k3s + MinIO(S3 호환) + Jupyter. `spark-submit --deploy-mode cluster`로 k8s 팟을 동적 생성해 처리하는 실무형 파이프라인(Bronze/Silver/Gold) 실습 |
| [`03/`](03) | 오케스트레이션(Airflow) | `02`의 k3s/MinIO를 그대로 재사용하면서, Airflow `KubernetesPodOperator`로 ingest→Bronze→Silver→Gold 전체를 DAG(`medallion_pipeline`)로 자동화 |

`00`, `01`은 동일한 구조(`docker-compose.yml`, `requirements.txt`, `실습코드/`)를 가지며, 각자 독립적으로 `docker compose up`으로 띄울 수 있습니다. `02`는 별도로 MinIO와 k3s가 추가된 구조이고, `03`은 `02`의 네트워크/볼륨을 참조해 Airflow만 추가로 얹은 구조입니다(아래 참고).

### 00 — 싱글 노드 (`00/docker-compose.yml`)

- 이미지: `jupyter/all-spark-notebook:latest`, 컨테이너 1개(`jupyter-ml`)
- 포트: `9999 → 8888` (Jupyter 웹 UI)
- 볼륨: `./실습코드` → `/home/jovyan/work`

```bash
cd 00
docker compose up
# http://localhost:9999 접속 (토큰 없음)
```

### 01 — 멀티 노드 클러스터 (`01/docker-compose.yml`)

- 이미지: `jupyter/all-spark-notebook:latest`, 컨테이너 3개
  - `jupyter-ml`: Spark 마스터 겸 Jupyter 서버
  - `spark-worker-1`, `spark-worker-2`: Spark 워커 (각 2g 메모리 제한)
- 포트: `9999 → 8888`(Jupyter), `4040`(Spark 작업 모니터링 UI), `8080`(Spark 마스터 관리자 UI)
- 워커 컨테이너도 동일한 `./실습코드` 볼륨을 마운트하여 마스터와 같은 노트북 파일을 바라봄

```bash
cd 01
docker compose up
# http://localhost:9999 : Jupyter
# http://localhost:8080 : Spark 마스터 UI
# http://localhost:4040 : Spark Job UI
```

### 02 — Kubernetes(k3s) 클러스터 모드 (`02/docker-compose.yml`)

로컬 `docker-compose`로 "가짜 프로덕션" 환경(k8s 클러스터 + S3 호환 오브젝트 스토리지)을 흉내내서, 실무의 spark-submit 배포 방식을 그대로 연습하는 구성.

- 컨테이너 3개
  - `k8s-master-node` (`rancher/k3s`): 로컬 가상 쿠버네티스 클러스터. Jupyter가 여기로 작업을 제출하면 driver/executor 팟을 동적으로 생성함
  - `k8s-spark-jupyter` (`jupyter/base-notebook` + PySpark 3.4.1 자동 설치): 코드 작성/로컬(`local[*]`) 검증용 클라이언트
  - `k8s-s3-minio`: S3 호환 오브젝트 스토리지. 원본 데이터/코드/결과물을 모두 여기(`test-bucket`, `code-bucket`)에 저장
- 포트: `9999→8888`(Jupyter), `6443`(k8s API), `9000`/`9001`(MinIO API/콘솔)
- MinIO는 `./minio_data` 볼륨 마운트라 컨테이너 재생성에도 데이터가 살아남음. **k3s(`k8s-master-node`)는 상태가 볼륨에 안 남아서, Docker Desktop이 재시작되면 RBAC/배포된 서비스가 전부 초기화됨** (복구 절차는 `02/start.md` 13번 참고)

```bash
cd 02
docker compose up
# http://localhost:9999 : Jupyter (로컬 검증용)
# http://localhost:9001 : MinIO 콘솔 (minioadmin/minioadmin)
```

**아키텍처 — Bronze / Silver / Gold 메달리온**

| 레이어 | 경로 | 내용 |
| --- | --- | --- |
| Bronze | `s3a://test-bucket/bronze/cluster_mode_result` | 원본 CSV(8.4GiB)를 Parquet으로 1회 변환 (2.3GiB) |
| Silver | `s3a://test-bucket/silver/ecommerce_refined` | product_id/category_id 기준 self-join으로 category_code·brand 결측치 보강 + 정제 |
| Gold | `s3a://test-bucket/gold/*` | 이벤트 퍼널, 카테고리/브랜드별 매출, 일별 추이, 가격 기술통계 등 집계 테이블 8종 |

**실행 방식 2가지**
- **로컬 검증** (`*.ipynb`, `master("local[*]")`): 코드 개발/디버깅, 샘플 데이터로 로직 검증, 이미 계산 끝난 Gold 테이블 눈으로 확인할 때
- **클러스터 제출** (`*.py`, `spark-submit --deploy-mode cluster`): MinIO의 `code-bucket`에 업로드 후 `kubectl run`으로 제출, 실제 대용량 데이터 처리 및 최종 검증

### 03 — Airflow 오케스트레이션 (`03/docker-compose.yml`)

`02`까지는 사람이 손으로 `_SUCCESS` 확인 후 다음 `.py`를 제출했음. `03`은 이 수동 순서를 Airflow DAG(`medallion_pipeline`)로 자동화한 것 — k3s/MinIO를 새로 안 띄우고 `02`가 만든 네트워크·볼륨을 `external: true`로 재사용하므로, **`03`을 띄우기 전에 `02`가 먼저 떠 있어야 함**.

- 컨테이너 3개
  - `airflow-kubeconfig-prep` (1회성): `02`의 k3s가 만든 kubeconfig 서버 주소(`127.0.0.1:6443`, k3s 컨테이너 자신 기준)를 `k8s-master-node`로 치환해 Airflow가 쓸 수 있게 변환
  - `airflow-init` (1회성): DB 마이그레이션 + 고정 계정(`admin`/`1234`, `03/.env`) 생성
  - `airflow-standalone`: 웹서버+스케줄러+트리거러 통합 실행 (`SequentialExecutor` + sqlite)
- 포트: `8090 → 8080` (Airflow 웹 UI)
- DAG 흐름: `ingest_raw_data`(Kaggle CSV 다운로드) → `bronze_to_parquet` → `silver_refine` → `gold_stats`, 각 태스크는 `KubernetesPodOperator`로 k3s에 팟을 생성해 실행 (Spark 3개 태스크는 실제로 `spark-submit --deploy-mode cluster`를 다시 제출)
- 자격증명(Kaggle API, MinIO)은 DAG 코드에 안 적고 k3s Secret(`kaggle-credentials`, `minio-credentials`)으로만 주입

```bash
cd 03
docker compose up -d
# http://localhost:8090 : Airflow 웹 UI (admin/1234)
```

처음부터 재현하는 전체 절차(사전 준비물, RBAC/Secret 생성, code-bucket 업로드 등)는 **[`03/실행_가이드.md`](03/실행_가이드.md)** 참고. 그 외 문서:

| 문서 | 내용 |
| --- | --- |
| [`03/이론.md`](03/이론.md) | DAG/Task/Operator, 스케줄링(`start_date`/`schedule`/`catchup`), Executor 종류, 멱등성, XCom 등 오케스트레이션 개념 정리 |
| [`03/파이프라인_설계.md`](03/파이프라인_설계.md) | `medallion_pipeline.py` 설계 결정(설정값 상수화, 태스크 팩토리 함수, 태스크 분리 기준, 자격증명 흐름 등)과 현재 설계의 한계 |
| [`03/함수.md`](03/함수.md) | 실제 사용한 Airflow API(`KubernetesPodOperator`, `Secret`, `>>` 등) 정리 |
| [`03/명령어.md`](03/명령어.md) | `docker compose`, Airflow CLI, k3s 팟 확인, code-bucket 업로드 명령 정리 |
| [`03/시행착오_0803-0804.md`](03/시행착오_0803-0804.md) | Airflow 도입 시행착오(kubeconfig 변환, provider 미설치, `--unzip` 조용한 실패, MinIO IP 불일치로 인한 job 무한 대기 등) |

## 공통 의존성 (`requirements.txt`)

컨테이너 기동 시 자동 설치됩니다 (numpy ABI 충돌 방지를 위해 핵심 수치 스택은 버전 고정).

- 코어: `numpy`, `pandas`, `scipy`, `matplotlib`, `scikit-learn`, `statsmodels`
- 머신러닝: `xgboost`, `lightgbm`, `imbalanced-learn`
- 시각화/분석: `seaborn`, `mlxtend`, `joblib`, `ydata-profiling`

## 실습 데이터

- `행정안전부_착한가격업소 현황_20260630.csv` — 행정안전부의 "착한가격업소" 현황 데이터(시도/시군/업종/업소명/연락처/주소/메뉴·가격 등 12,645행). 00, 01 양쪽 실습코드 폴더에 각각 위치.
- `2019-Nov.csv` — 이커머스 이벤트 로그(event_time/event_type/product_id/category_id/category_code/brand/price/user_id/user_session, 8.4GiB). `02`에서 MinIO(`test-bucket`)에 올려두고 Bronze/Silver/Gold 파이프라인 실습에 사용.
- `03`에서는 사람이 직접 올리는 대신 `ingest_raw_data` 태스크가 Kaggle API로 같은 계열의 파일(기본 `2019-Oct.csv`, 트리거 시 `kaggle_file` 파라미터로 다른 달 지정 가능)을 매번 새로 받아 MinIO에 적재.

## 노트북 실습 내용

### `00/실습코드/SPARK/00/`

- **`01.ipynb`** — `SparkSession` 생성부터 시작: 로컬 CSV(착한가격업소 데이터) 로드, 스키마/행 수 확인, `업종 == 양식` 필터링 후 결과를 CSV로 저장
- **`02.ipynb`** — `SparkContext`를 이용한 로우레벨 RDD 실습: `textFile`/`parallelize` → `flatMap`/`map`/`reduceByKey`로 워드카운트 구현 (MapReduce 개념 설명 포함)
- **`03.ipynb`** — `SparkSession.master("local[*]")`로 전체 코어 활용, RDD 워드카운트·DataFrame 생성/필터링·`createOrReplaceTempView` + Spark SQL 조회 실습, 셔플링(Shuffling)과 캐싱(`.cache()`) 개념 정리
- **`func.ipynb`** — Transformation vs Action API 정리 노트: `map`/`filter`/`union`/`distinct` 등 트랜스포메이션, `collect`/`count`/`reduce`/`take` 등 액션, RDD 생성 방법(`parallelize`/`range`/`textFile`) 및 Lazy Evaluation·RDD Lineage 설명
- 실행 결과물로 `output.csv/` (양식 업종 필터링 결과) 생성됨

### `01/실습코드/01/`

- **`01.ipynb`** — `spark://jupyter:7077` 클러스터 마스터에 연결하여 워커 분산 처리 실습. 착한가격업소 CSV 로드(스키마/행 수/미리보기 확인) → `inferSchema` 끄고 `repartition(4)`로 파티션 강제 분할 → 가격 컬럼 정수 변환 후 `rollup("업종", "시도")`로 업종별·업종+시도별 평균 가격 집계

### `02/실습코드/01/` — Kubernetes 클러스터 모드, Bronze/Silver/Gold 파이프라인

이커머스 이벤트 로그(`2019-Nov.csv`, Kaggle "eCommerce behavior data" 계열, product_id/event_type/category_code/brand/price 등)를 대상으로 실무형 배치 파이프라인을 구성. `.py`는 `spark-submit --deploy-mode cluster`로 k3s에 제출하는 실행 파일, `.ipynb`는 로컬(`local[*]`) 개발·검증용.

| 파일 | 역할 |
| --- | --- |
| `start.py` | **Bronze**: 원본 CSV(8.4GiB) → Parquet(2.3GiB) 1회 변환. 이후 모든 job이 CSV 대신 이걸 읽어서 파싱/스키마추론 비용을 줄임 |
| `01.py` / `01.ipynb` | k3s 클러스터 모드 첫 실습 — S3A(MinIO) 연동 설정, `category_code`별 count 집계 후 저장. spark-submit 배포 시행착오(RBAC, driver 이미지 지정, DNS resolve 문제 등)의 대상 코드 |
| `01View.ipynb` | 로컬 세션에서 `01.py`가 저장한 결과를 다시 읽어 확인하는 검증용 노트북 |
| `02.py` / `02.ipynb` | **Silver**: Bronze parquet을 읽어 product_id/category_id 기준 self-join으로 `category_code`/`brand` 결측치 보강 → 정제된 데이터를 `category_code`로 파티셔닝해 저장. `.repartition('category_code')`로 파일 개수·커밋 시간 최적화 |
| `03.py` / `03.ipynb` | **Gold**: Silver를 읽어 이벤트 퍼널, 카테고리/브랜드별 매출 Top N, 일별 추이, 가격 기술통계 등 8개 집계 테이블을 생성해 저장 |
| `03View.ipynb` | `02.ipynb`/`03.ipynb` 개발 과정에서 로컬 세션으로 중간 결과를 확인한 노트북 |
| `resultView.ipynb` | Gold 레이어 결과만 로컬(`local[*]`)로 읽어서 눈으로 확인하는 노트북 (Gold는 이미 계산 끝난 작은 집계 결과라 로컬로 읽어도 빠름) |

파이프라인 전체의 시행착오 히스토리(spark-submit 튜닝, S3A 자격증명, 모니터링 UI 설정, self-join row 폭발 디버깅, 로컬 리소스 부족으로 인한 크래시와 최적화, Docker Desktop 재시작 후 클러스터 복구 절차 등)는 **[`02/start.md`](02/start.md)** 에 상세히 기록되어 있습니다.

### `03/dags/`, `03/ingest/` — Airflow DAG 정의

`02/실습코드/01/`의 Bronze/Silver/Gold 스크립트(`start.py`/`02.py`/`03.py`)를 수정 없이 그대로 재사용하고, 그 앞뒤로 자동화 레이어만 추가한 구조.

| 파일 | 역할 |
| --- | --- |
| `dags/medallion_pipeline.py` | DAG 본체. 설정 상수, `KubernetesPodOperator` 팩토리 함수(`make_task`), 4개 태스크(`ingest_raw_data >> bronze_to_parquet >> silver_refine >> gold_stats`) 정의 |
| `ingest/ingest.py` | `ingest_raw_data` 태스크가 부트스트랩으로 내려받아 실행하는 실제 로직. Kaggle API로 CSV(zip) 다운로드 후 `zipfile`로 직접 압축 해제(`--unzip` 옵션이 조용히 실패하는 문제 우회), MinIO에 업로드 |
| `upload_to_bucket.py` (03 루트) | `02/실습코드/01/*.py` + `ingest/ingest.py`를 `code-bucket`에 재업로드하는 스크립트. 로컬 코드를 고친 뒤 반드시 실행해야 실제 실행 대상에 반영됨 |

파이프라인 전체의 시행착오 히스토리(Airflow 도입기, kubeconfig 변환, MinIO IP 불일치로 인한 job 무한 대기 디버깅 등)는 **[`03/시행착오_0803-0804.md`](03/시행착오_0803-0804.md)** 에 기록되어 있습니다.

## 요구 사항

- Docker / Docker Compose
- (03 실습 시) Kaggle API 토큰(username, key) — https://www.kaggle.com/settings → API → Create New Token
