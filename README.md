# pyspark

PySpark 학습용 실습 저장소. Docker 기반 Jupyter + Spark 환경에서 RDD/DataFrame/Spark SQL을 단계별로 실습합니다.

## 구성

| 폴더 | 환경 | 설명 |
| --- | --- | --- |
| [`00/`](00) | 싱글 노드 | Jupyter 한 컨테이너에서 Spark를 로컬(`local`) 모드로 실행. RDD/DataFrame 기본기 실습 |
| [`01/`](01) | 멀티 노드 클러스터 | Jupyter(마스터) + Spark 워커 2대로 구성된 클러스터. 실제 클러스터 연결·분산 처리 실습 |

두 폴더 모두 동일한 구조(`docker-compose.yml`, `requirements.txt`, `실습코드/`)를 가지며, 각자 독립적으로 `docker compose up`으로 띄울 수 있습니다.

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

## 공통 의존성 (`requirements.txt`)

컨테이너 기동 시 자동 설치됩니다 (numpy ABI 충돌 방지를 위해 핵심 수치 스택은 버전 고정).

- 코어: `numpy`, `pandas`, `scipy`, `matplotlib`, `scikit-learn`, `statsmodels`
- 머신러닝: `xgboost`, `lightgbm`, `imbalanced-learn`
- 시각화/분석: `seaborn`, `mlxtend`, `joblib`, `ydata-profiling`

## 실습 데이터

`행정안전부_착한가격업소 현황_20260630.csv` — 행정안전부의 "착한가격업소" 현황 데이터(시도/시군/업종/업소명/연락처/주소/메뉴·가격 등 12,645행). 00, 01 양쪽 실습코드 폴더에 각각 위치.

## 노트북 실습 내용

### `00/실습코드/SPARK/00/`

- **`01.ipynb`** — `SparkSession` 생성부터 시작: 로컬 CSV(착한가격업소 데이터) 로드, 스키마/행 수 확인, `업종 == 양식` 필터링 후 결과를 CSV로 저장
- **`02.ipynb`** — `SparkContext`를 이용한 로우레벨 RDD 실습: `textFile`/`parallelize` → `flatMap`/`map`/`reduceByKey`로 워드카운트 구현 (MapReduce 개념 설명 포함)
- **`03.ipynb`** — `SparkSession.master("local[*]")`로 전체 코어 활용, RDD 워드카운트·DataFrame 생성/필터링·`createOrReplaceTempView` + Spark SQL 조회 실습, 셔플링(Shuffling)과 캐싱(`.cache()`) 개념 정리
- **`func.ipynb`** — Transformation vs Action API 정리 노트: `map`/`filter`/`union`/`distinct` 등 트랜스포메이션, `collect`/`count`/`reduce`/`take` 등 액션, RDD 생성 방법(`parallelize`/`range`/`textFile`) 및 Lazy Evaluation·RDD Lineage 설명
- 실행 결과물로 `output.csv/` (양식 업종 필터링 결과) 생성됨

### `01/실습코드/01/`

- **`01.ipynb`** — `spark://jupyter:7077` 클러스터 마스터에 연결하여 워커 분산 처리 실습. 착한가격업소 CSV 로드(스키마/행 수/미리보기 확인) → `inferSchema` 끄고 `repartition(4)`로 파티션 강제 분할 → 가격 컬럼 정수 변환 후 `rollup("업종", "시도")`로 업종별·업종+시도별 평균 가격 집계

## 요구 사항

- Docker / Docker Compose
