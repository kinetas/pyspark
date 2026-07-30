# Spark on Kubernetes 실습 정리

## 시작 전 개요

- 시작할 때 MinIO로 버킷을 먼저 만들어줘야 함.
- 버킷은 논리적으로 물리적 저장소를 묶은 것. 버킷에 저장해도 경로를 통해 다른 저장장치에 저장 가능.
- 버킷을 한 개만 두는 경우도 있지만, 여러 아키텍처가 존재함.
- 메달리온 아키텍처는 브론즈/실버/골드로 나눈 아키텍처.

---

## 시행착오 히스토리 (spark-submit 제출 과정)

### 시도 1: Jupyter 인터프리터 환경에서 client 모드로 실행

**문제점**: 쿠버네티스 권한 문제. 배포모드를 주피터 인터프리터 환경에서 client로 하니까 문제가 발생함.

**해결 방향**:
- 권한 설정을 해주거나 RBAC 형태로 쿠버네티스 안에 권한이 허용되는 역할을 만든 후 그 권한으로 실행하는 방식
- 세션을 생성해서 인터프리터 환경에서 하지 말고, 그냥 명령어로 cluster 형태로 실행

```
docker exec -it k8s-spark-jupyter bash
```

```
/opt/spark/bin/spark-submit \
  --master k8s://https://k8s-cluster:6443 \
  --deploy-mode cluster \
  --name spark-production-job \
  --class org.apache.spark.deploy.k8s.submit.KubernetesClientApplication \
  --conf spark.kubernetes.container.image=datamechanic/spark:3.4.1-hadoop-3.3.2-java-11-scala-2.12-latest \
  --conf spark.executor.instances=2 \
  --conf spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
  --conf spark.kubernetes.trustcerts=true \
  local:///home/jovyan/work/01.py
```

**결과**: 실패. 이미지 내부에 스파크(Spark) 실행 바이너리 파일들이 아예 설치되어 있지 않아서 발생한 문제.

### 시도 2: k8s-master-node에서 kubectl run으로 직접 제출

쿠버네티스에서 실행:

```
docker exec -it k8s-master-node sh
```

★ 핵심: 주피터의 local 경로가 아닌 가상 S3 버킷에 저장된 코드 경로를 직접 사용. 주피터 컨테이너는 누가 고쳤는지 불확실하고 설정 바꾸다가 망가질 위험이 있으므로, 완성된 코드만 S3에 넣어서 실행.

```
kubectl run spark-production-pipeline \
  --image=datamechanic/spark:3.4.1-hadoop-3.3.2-java-11-scala-2.12-latest \
  --restart=Never \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "submitter",
        "image": "datamechanic/spark:3.4.1-hadoop-3.3.2-java-11-scala-2.12-latest",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.executor.instances=2",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

**결과**: 실패. 이미지 문제.

```
kubectl delete pod spark-production-pipeline --force
```

일단 문제의 팟 제거.

### 시도 3: 공식 apache/spark 이미지로 교체

```
kubectl run spark-production-pipeline \
  --image=apache/spark:3.4.1 \
  --restart=Never \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "submitter",
        "image": "apache/spark:3.4.1",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://kubernetes.default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.executor.instances=2",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

이미지 수정 후 재실행. 컨테이너에서 에러 발생.

### 시도 4: ServiceAccount 지정 + ivy 경로 격리

```
# 1. 터져서 찌꺼기가 남은 팟을 완전히 삭제합니다.
kubectl delete pod spark-production-pipeline --force

# 2. 이번에는 컨테이너를 직접 띄우지 않고, K8s 내부의 '서비스 계정(ServiceAccount)' 권한을 사용하여
# 스파크 공식 재단이 권장하는 규격대로 부드럽게 팟을 생성 요청합니다.
# 핵심: 다운로드 저장소 내부 충돌을 방지하기 위해 ivy 경로를 임시 폴더로 격리 지정합니다.
kubectl run spark-production-pipeline \
  --image=apache/spark:3.4.1 \
  --restart=Never \
  --overrides='{
    "spec": {
      "serviceAccountName": "default",
      "containers": [{
        "name": "submitter",
        "image": "apache/spark:3.4.1",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.executor.instances=2",
          "--conf", "spark.jars.ivy=/tmp/.ivy",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

**결과**: 실패.

로그 확인 결과 (`kubectl logs spark-production-pipeline`):

```
Exception in thread "main" org.apache.spark.SparkException: Must specify the driver container image
```

**원인**: `kubectl run`의 `--image`는 spark-submit이 도는 submitter 팟용 이미지일 뿐, `--deploy-mode cluster`에서 spark-submit이 새로 만드는 driver 팟에는 별도로 이미지를 지정해줘야 함 (`spark.kubernetes.container.image` conf 누락).

참고: `k8s://https://default.svc` 마스터 URL은 팟 내부 서비스어카운트로 자동 감지되어 실제 문제는 아니었음 ("Auto-configuring K8S client using current context from users K8S config file" 로그로 확인).

### 시도 5: driver container image 지정

**해결**: args에 `spark.kubernetes.container.image` conf 추가.

```
kubectl delete pod spark-production-pipeline --force

kubectl run spark-production-pipeline \
  --image=apache/spark:3.4.1 \
  --restart=Never \
  --overrides='{
    "spec": {
      "serviceAccountName": "default",
      "containers": [{
        "name": "submitter",
        "image": "apache/spark:3.4.1",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://kubernetes.default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.kubernetes.container.image=apache/spark:3.4.1",
          "--conf", "spark.executor.instances=2",
          "--conf", "spark.jars.ivy=/tmp/.ivy",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

**결과**: 실패 (재실행 확인, driver image 문제는 해결됨).

로그 확인 결과:

```
ERROR Client: Please check "kubectl auth can-i create pod" first. It should be yes.
Exception in thread "main" io.fabric8.kubernetes.client.KubernetesClientException: ...
Forbidden! ... User "system:serviceaccount:default:default" cannot create resource "pods"
in API group "" in the namespace "default".
```

**원인**: `serviceAccountName: default`는 팟을 생성할 RBAC 권한이 없음. Spark driver는 executor 팟을 직접 생성해야 하므로 이 권한이 없으면 실패함. (시도 1에서 메모해뒀던 그 권한 문제와 동일)

### 시도 6: 전용 ServiceAccount + RBAC 부여

**해결**: Spark 전용 서비스어카운트 생성 + 권한 부여 (k8s-master-node 안에서 1회만 실행하면 됨).

```
kubectl create serviceaccount spark --namespace=default
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

그 다음 `kubectl run` 명령의 `serviceAccountName`을 `spark`로 변경:

```
kubectl delete pod spark-production-pipeline --force

kubectl run spark-production-pipeline \
  --image=apache/spark:3.4.1 \
  --restart=Never \
  --overrides='{
    "spec": {
      "serviceAccountName": "spark",
      "containers": [{
        "name": "submitter",
        "image": "apache/spark:3.4.1",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://kubernetes.default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.kubernetes.container.image=apache/spark:3.4.1",
          "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
          "--conf", "spark.executor.instances=2",
          "--conf", "spark.jars.ivy=/tmp/.ivy",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

**결과**: RBAC 문제는 해결됨 (submitter 팟 Completed). 대신 driver 팟(`s3-code-execution-job-...-driver`)이 Error.

로그 확인 결과:

```
Exception in thread "main" java.nio.file.AccessDeniedException: s3a://code-bucket/01.py:
NoAuthWithAWSException: No AWS Credentials provided ...
```

**원인 1**: spark-submit이 실행할 `01.py` 자체를 `s3a://code-bucket/01.py`에서 driver 팟으로 내려받는 과정은 파이썬 코드가 실행되기 "이전" 단계라서, `01.py` 안에서 `hadoop_conf.set(...)`으로 자격증명을 넣어봐야 소용없음. spark-submit 명령의 `--conf`로 미리 넘겨줘야 함.

**원인 2**: `01.py` / `docker-compose.yml`에서 쓰는 엔드포인트 호스트명 `minio-storage`는 도커 임베디드 DNS(`127.0.0.11`, `k8s-master-node` 컨테이너 안에서만 유효)에만 등록되어 있어서 k3s 팟 네트워크에서는 resolve 안 됨.

확인: `docker inspect k8s-s3-minio` → IP `172.19.0.3` (`spark_on_kubernetes_cluster_default` 네트워크). 테스트 팟에서 `wget http://172.19.0.3:9000/minio/health/live` → 200 OK로 접속 가능 확인됨.

> 주의: 이 IP는 컨테이너 재생성 시 바뀔 수 있음. `docker inspect k8s-s3-minio`로 재확인 필요.

### 시도 7 (최종 성공): S3A 자격증명/엔드포인트 conf 추가 + 재업로드

**해결**:
1. `02/실습코드/01/01.py` 안의 `fs.s3a.endpoint`를 `http://minio-storage:9000` → `http://172.19.0.3:9000`으로 수정 (완료)
2. 수정된 `01.py`를 `code-bucket`에 다시 업로드 (기존 `minio_data/code-bucket/01.py`는 예전 버전이라 반드시 재업로드 필요)
   - Jupyter(`k8s-spark-jupyter`, `localhost:9999`)에서 mc 또는 boto3로 재업로드
   - mc client 사용 예시:
     ```
     mc alias set local http://localhost:9000 minioadmin minioadmin
     mc cp "02/실습코드/01/01.py" local/code-bucket/01.py
     ```
3. spark-submit에도 `--conf`로 S3A 자격증명/엔드포인트 추가 (`01.py` 다운로드 자체에 필요)

```
kubectl delete pod spark-production-pipeline --force
kubectl delete pod s3-code-execution-job-1716f19fb0cea1f4-driver --force

kubectl run spark-production-pipeline \
  --image=apache/spark:3.4.1 \
  --restart=Never \
  --overrides='{
    "spec": {
      "serviceAccountName": "spark",
      "containers": [{
        "name": "submitter",
        "image": "apache/spark:3.4.1",
        "command": ["/opt/spark/bin/spark-submit"],
        "args": [
          "--master", "k8s://https://kubernetes.default.svc",
          "--deploy-mode", "cluster",
          "--name", "s3-code-execution-job",
          "--conf", "spark.kubernetes.container.image=apache/spark:3.4.1",
          "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
          "--conf", "spark.dynamicAllocation.enabled=true",
          "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=true",
          "--conf", "spark.dynamicAllocation.minExecutors=1",
          "--conf", "spark.dynamicAllocation.maxExecutors=4",
          "--conf", "spark.dynamicAllocation.initialExecutors=1",
          "--conf", "spark.jars.ivy=/tmp/.ivy",
          "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
          "--conf", "spark.hadoop.fs.s3a.endpoint=http://172.19.0.3:9000",
          "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
          "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
          "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
          "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
          "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
          "s3a://code-bucket/01.py"
        ]
      }]
    }
  }'
```

**결과**: 성공. driver 팟 + executor 팟 모두 `Running` → 최종 `Completed`, `test-bucket/output/cluster_mode_result/`에 `_SUCCESS`와 parquet 파일 정상 생성 확인됨.

---

## 실행 후 Q&A 정리 (모니터링, 결과 확인, 로컬 vs 클러스터)

### 1. 동적 할당(Dynamic Allocation) 적용

`spark.executor.instances=2` (고정 개수) 대신 아래로 교체함:

```
"--conf", "spark.dynamicAllocation.enabled=true",
"--conf", "spark.dynamicAllocation.shuffleTracking.enabled=true",
"--conf", "spark.dynamicAllocation.minExecutors=1",
"--conf", "spark.dynamicAllocation.maxExecutors=4",
"--conf", "spark.dynamicAllocation.initialExecutors=1",
```

`shuffleTracking.enabled=true`가 필수인 이유: Kubernetes에는 YARN 같은 External Shuffle Service가 없어서, 이게 없으면 idle executor가 shuffle 데이터를 들고 있어도 바로 반납되어 재계산이 발생함.

### 2. 모니터링 UI (Spark UI, 4040 포트) 접속 방법

- `docker-compose.yml`에 `4040:4040`을 추가해도 소용없음: driver는 jupyter 컨테이너가 아니라 k3s 클러스터 안에서 동적으로 생성되는 팟이라 컨테이너 포트 매핑과 무관함.
- driver 실행 시 `<job-name>-driver-svc`라는 ClusterIP 서비스가 자동 생성되며 4040 포트를 가지고 있음.
- 클러스터를 재시작(=현재 돌고 있는 job이 죽음)하지 않고 보는 방법: 호스트 kubectl로 직접 port-forward.
  1. k3s의 kubeconfig를 컨테이너에서 호스트로 복사 (server 주소를 `https://localhost:6443`으로 수정):
     ```
     docker cp k8s-master-node:/etc/rancher/k3s/k3s.yaml ~/.kube_pysprak02/k3s.yaml
     ```
  2. 호스트에서:
     ```
     export KUBECONFIG=~/.kube_pysprak02/k3s.yaml
     kubectl port-forward svc/<job-name>-driver-svc 4040:4040
     ```
  3. 브라우저에서 http://localhost:4040 접속.
- 참고: 다음에 클러스터를 새로 올릴 때부터는 편의를 위해 `02/docker-compose.yml`의 `k8s-cluster` 서비스에 `"4040:4040"`, `"8080:8080"` 포트를 미리 추가해두는 것도 방법 (단, 지금 추가하려면 컨테이너 재생성이 필요해서 현재 작업이 끊기므로 작업이 다 끝난 뒤에 적용할 것).
- **단점**: job이 끝나면 driver-svc가 사라지고, 매 제출마다 서비스 이름이 랜덤이라 매번 새로 port-forward 해야 함. **재설정 없이 계속 모니터링하고 싶으면 8번 섹션(지난 기록, Spark History Server)과 9번 섹션(실시간, 라벨 기반 고정 Service)을 참고.**

### 3. Jupyter에서 결과 확인 (`02/실습코드/01/01View.ipynb`)

- 처음엔 `hadoop_conf.set("fs.s3a.impl", ...)` 방식으로 시도했으나 `ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found` 발생.
- 원인: `hadoop_conf.set()`은 SparkSession이 이미 생성된 뒤 설정값만 바꾸는 것이라, S3A 관련 jar를 classpath에 새로 추가하지 못함. `spark.jars.packages`는 반드시 `SparkSession.builder.config(...)`로 **세션 생성 전에** 지정해야 함.
- 수정한 최종 코드:

  ```python
  from pyspark.sql import SparkSession

  spark = SparkSession.builder \
      .appName("CheckResult") \
      .master("local[*]") \
      .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio-storage:9000") \
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
      .config("spark.hadoop.fs.s3a.path.style.access", "true") \
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
      .getOrCreate()

  df = spark.read.parquet("s3a://test-bucket/output/cluster_mode_result")
  df.show()
  ```

- 주의: 이미 커널에서 세션이 한 번 생성된 상태라면 `.config()`를 아무리 바꿔도 반영 안 됨. 반드시 Kernel → Restart Kernel 후 첫 셀로 실행해야 함.
- 이 코드는 `master("local[*]")`라 Jupyter 컨테이너 안에서만 도는 로컬 세션이므로 `minio-storage` 호스트명 그대로 써도 됨 (같은 docker-compose 네트워크라 정상 resolve됨). k3s 팟 안에서 돌 때만 IP(`172.19.0.3`)로 바꿔야 했던 것과 다른 상황.

### 4. "로컬로 결과 확인할 때는 쿠버네티스를 쓰는 건가?" 에 대한 정리

- `master("local[*]")`는 쿠버네티스를 전혀 쓰지 않음. Jupyter 컨테이너 안의 JVM 하나가 로컬 CPU 코어 수만큼 스레드를 만들어 driver 역할과 executor 역할을 전부 혼자 처리함. 팟도 생성되지 않음.
- 로컬 모드에서도 이론적으로 `.master("k8s://https://kubernetes.default.svc")` + `.config("spark.submit.deployMode", "client")`로 클러스터를 쓰게 만들 수는 있음. 하지만 이게 바로 시도 1에서 처음 겪었던 "client 모드 문제"의 원인과 같은 종류임: driver가 Jupyter 컨테이너 안에 있고 k8s가 만든 executor 팟들이 그 driver에게 역으로 접속해야 하므로 `spark.driver.host`/`spark.driver.port` 고정, 네트워크 도달성 등 추가로 풀어야 할 문제가 다시 생김.
- → 그래서 무거운 job은 지금 방식(cluster 모드, kubectl run으로 명령어 제출)을 쓰고, 결과 확인 같은 가벼운 작업만 로컬 모드를 쓰는 것으로 정리함.

### 5. 개발 워크플로우 정리

- 코드 개발/디버깅 단계: `local[*]`로 빠르게 반복 (팟 생성/이미지 다운로드 없어서 훨씬 빠름).
- 로직이 완성되면: 코드를 S3(code-bucket)에 올리고 cluster 모드로 최종 실행해서 검증.
- 주의: 로컬에서 통과했다고 cluster에서도 100% 통과하는 건 아님. 이번에 겪은 문제들(driver 이미지 미지정, RBAC, `minio-storage` 호스트명 미resolve)은 전부 로컬 모드에서는 애초에 발생하지 않는 종류의 에러였음 — 로컬은 팟도 안 만들고 네트워크도 컨테이너 안에서 다 해결되기 때문. 즉 로컬 검증은 "로직이 맞는가"만 확인해주고, "cluster 환경 설정이 맞는가"는 최소 1번은 실제 cluster 제출로 확인해야 함.

### 6. 로컬 모드 vs 클러스터 모드 최종 비교

| 구분 | 로컬 모드 (`local[*]`) | 클러스터 모드 (`k8s://...`, `--deploy-mode cluster`) |
|---|---|---|
| 실행 위치 | Jupyter 컨테이너 안 JVM 하나 | k8s가 동적으로 만드는 driver 팟 + executor 팟들 |
| 쿠버네티스 사용 여부 | 사용 안 함 | 사용함 (팟 생성/스케줄링) |
| RBAC 권한 필요 여부 | 불필요 | 필요 (팟 생성 권한 있는 ServiceAccount 필수) |
| 실행 속도 (기동) | 즉시 (JVM 하나만 뜸) | 느림 (이미지 pull, 팟 스케줄링, executor 기동 등 30초~수 분) |
| 네트워크 이슈 | 거의 없음 (같은 컨테이너/네트워크 안) | 있음 (팟 네트워크에서 도커 호스트명 DNS resolve 안 됨 → IP 직접 사용 필요) |
| S3 접근 설정 시점 | 코드 실행 중 아무 때나 설정 가능 (단, `spark.jars.packages`는 세션 생성 전 필수) | spark-submit 실행 시점에 `--conf`로 미리 줘야 함 (원본 스크립트 파일 자체를 S3에서 받아오는 과정이 코드 실행 전이라서) |
| 확장성/병렬성 | 로컬 머신 코어 수로 제한 | executor 개수만큼 분산 처리, 동적 할당으로 자동 확장/축소 가능 |
| 모니터링 UI (4040) | 컨테이너 내부 포트라 `docker-compose.yml`에 포트 매핑 없으면 호스트에서 못 봄 | `<job>-driver-svc` 서비스 생성됨, `kubectl port-forward`로 조회 가능 |
| 용도 | 코드 개발/디버깅, 결과 빠른 확인 | 실제 운영/대용량 데이터 처리, 최종 검증 |
| 장애 발생 이력 | 없음 (`ClassNotFoundException`은 설정 순서 문제였고 쿠버네티스와 무관) | driver 이미지 미지정, RBAC 권한, S3 자격증명/DNS resolve 총 3건 발생 |

### 7. 실행 방법 요약 (Step by Step)

#### 7-1. 로컬 모드 실행 (코드 개발/디버깅, 결과 확인용)

1. 브라우저에서 Jupyter 접속: http://localhost:9999 (토큰 없음)
2. 노트북 열기 (예: `02/실습코드/01/01View.ipynb`, 또는 새 노트북 생성)
3. **커널이 이미 떠 있는 상태라면 먼저 Kernel → Restart Kernel** (이전 세션이 남아있으면 `.config()`가 무시됨)
4. 첫 셀에 아래 코드 실행 (`spark.jars.packages`는 반드시 `getOrCreate()` 전에 지정):
   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName("MyJob") \
       .master("local[*]") \
       .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
       .config("spark.hadoop.fs.s3a.endpoint", "http://minio-storage:9000") \
       .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
       .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
       .config("spark.hadoop.fs.s3a.path.style.access", "true") \
       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
       .getOrCreate()

   # 여기부터 실제 로직 작성/테스트
   ```
5. 로직 검증 끝나면 다음 단계(클러스터 모드)로 넘어가서 최종 확인.

#### 7-2. 클러스터 모드 실행 (실제 운영/최종 검증용)

1. **코드 완성**: `02/실습코드/01/01.py` 안에는 `master`/`deployMode` 설정을 넣지 않고, 파일 경로는 로컬이 아닌 S3 경로만 참조하도록 작성.
2. **MinIO IP 확인** (컨테이너 재생성 시 바뀔 수 있으므로 매번 확인 권장):
   ```
   docker inspect k8s-s3-minio --format '{{json .NetworkSettings.Networks}}'
   ```
3. **완성된 코드를 code-bucket에 업로드**:
   ```
   mc alias set local http://localhost:9000 minioadmin minioadmin
   mc cp "02/실습코드/01/01.py" local/code-bucket/01.py
   ```
4. **k8s-master-node 컨테이너 진입**:
   ```
   docker exec -it k8s-master-node sh
   ```
5. **(최초 1회만) Spark 전용 ServiceAccount + 권한 부여**:
   ```
   kubectl create serviceaccount spark --namespace=default
   kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
   ```
6. **기존 팟 정리 후 spark-submit 제출** (최종 성공한 형태, IP/버킷/파일명은 상황에 맞게 수정):
   ```
   kubectl delete pod spark-production-pipeline --force

   kubectl run spark-production-pipeline \
     --image=apache/spark:3.4.1 \
     --restart=Never \
     --overrides='{
       "spec": {
         "serviceAccountName": "spark",
         "containers": [{
           "name": "submitter",
           "image": "apache/spark:3.4.1",
           "command": ["/opt/spark/bin/spark-submit"],
           "args": [
             "--master", "k8s://https://kubernetes.default.svc",
             "--deploy-mode", "cluster",
             "--name", "s3-code-execution-job",
             "--conf", "spark.kubernetes.container.image=apache/spark:3.4.1",
             "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
             "--conf", "spark.dynamicAllocation.enabled=true",
             "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=true",
             "--conf", "spark.dynamicAllocation.minExecutors=1",
             "--conf", "spark.dynamicAllocation.maxExecutors=4",
             "--conf", "spark.dynamicAllocation.initialExecutors=1",
             "--conf", "spark.eventLog.enabled=true",
             "--conf", "spark.eventLog.dir=s3a://code-bucket/spark-events",
             "--conf", "spark.jars.ivy=/tmp/.ivy",
             "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
             "--conf", "spark.hadoop.fs.s3a.endpoint=http://172.19.0.3:9000",
             "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
             "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
             "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
             "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
             "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
             "s3a://code-bucket/01.py"
           ]
         }]
       }
     }'
   ```
7. **상태 확인**:
   ```
   kubectl get pods -A
   ```
   `<job-name>-driver`와 executor 팟들이 `Running` → `Completed`가 되는지 확인.
8. **에러 발생 시 로그 확인**:
   ```
   kubectl logs spark-production-pipeline
   kubectl logs <job-name>-driver
   ```
9. **모니터링**: 지난 기록은 8번 섹션(Spark History Server, http://localhost:18080), 지금 실시간으로 도는지는 9번 섹션(실시간 4040, http://localhost:4040)에서 확인. 둘 다 이미 설정돼 있다면 매번 다시 할 필요 없음.
10. **결과 확인**: MinIO 저장 경로에 `_SUCCESS` 파일과 결과 데이터가 생성됐는지 확인 (직접 확인하거나, 7-1의 로컬 모드로 `spark.read`해서 확인).

---

## 8. 지속적 모니터링: Spark History Server (재설정 없이 계속 보기)

기존 방식(4040 포트, `kubectl port-forward svc/<job-name>-driver-svc`)의 한계:
- driver 서비스는 job이 끝나면 같이 사라지고, 매 제출마다 이름이 랜덤(`<job-name>-<랜덤ID>-driver-svc`)이라 **매번 이름을 새로 찾아서 포트포워딩을 다시 해야 함**.
- 8080(Standalone Spark Master UI)은 지금 구조(k8s cluster 모드)에는 아예 해당하지 않음. 쿠버네티스 자체가 스케줄러 역할이라 Spark Master 프로세스가 없음.

**해결**: Spark History Server를 클러스터에 지속 실행되는 Deployment로 띄우고, event log를 MinIO에 남기도록 설정. 이러면 서비스 이름이 고정이라 포트포워딩을 **한 번만** 설정하면 이후 job을 몇 번을 돌리든 계속 그 화면에서 확인 가능. (단, 실시간 progress bar는 없고 완료된 stage/task 통계 위주로 보임. 진행 중인 job의 실시간 확인은 여전히 개별 driver-svc의 4040이 필요함.)

### 8-1. 설정 (최초 1회)

1. MinIO에 이벤트 로그 저장할 디렉토리(prefix) 준비:
   ```
   docker run --rm --network spark_on_kubernetes_cluster_default --entrypoint sh \
     -v "<임의 파일>:/upload/keep.txt" \
     minio/mc -c "mc alias set local http://minio-storage:9000 minioadmin minioadmin && mc cp /upload/keep.txt local/code-bucket/spark-events/.keep"
   ```
2. History Server를 k3s에 Deployment + Service로 배포 (`apache/spark:3.4.1` 이미지 재사용, 기동 시 hadoop-aws/aws-sdk jar를 curl로 받아옴):
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: spark-history-server
     namespace: default
   spec:
     replicas: 1
     selector:
       matchLabels: { app: spark-history-server }
     template:
       metadata:
         labels: { app: spark-history-server }
       spec:
         containers:
           - name: history-server
             image: apache/spark:3.4.1
             command: ["sh", "-c"]
             args:
               - |
                 curl -sL -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
                 curl -sL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
                 exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
             env:
               - name: SPARK_HISTORY_OPTS
                 value: >-
                   -Dspark.history.fs.logDirectory=s3a://code-bucket/spark-events
                   -Dspark.hadoop.fs.s3a.endpoint=http://172.19.0.3:9000
                   -Dspark.hadoop.fs.s3a.access.key=minioadmin
                   -Dspark.hadoop.fs.s3a.secret.key=minioadmin
                   -Dspark.hadoop.fs.s3a.path.style.access=true
                   -Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
                   -Dspark.hadoop.fs.s3a.connection.ssl.enabled=false
             ports:
               - containerPort: 18080
   ---
   apiVersion: v1
   kind: Service
   metadata:
     name: spark-history-server
     namespace: default
   spec:
     selector: { app: spark-history-server }
     ports:
       - port: 18080
         targetPort: 18080
   ```
   `kubectl apply -f history-server.yaml`로 적용.
3. 호스트에서 고정 서비스 이름으로 포트포워딩 (서비스 이름이 항상 `spark-history-server`로 고정이라 매번 이름 찾을 필요 없음):
   ```
   export KUBECONFIG=~/.kube_pysprak02/k3s.yaml
   nohup kubectl port-forward svc/spark-history-server 18080:18080 > ~/.kube_pysprak02/history-portforward.log 2>&1 &
   ```
4. 브라우저에서 http://localhost:18080 접속.

### 8-2. 이후 사용법

- **매번 할 일 없음.** History Server 팟과 port-forward 프로세스가 살아있는 한 http://localhost:18080 를 새로고침하면 최신 job이 계속 리스트에 쌓임.
- spark-submit에 `spark.eventLog.enabled=true` + `spark.eventLog.dir=s3a://code-bucket/spark-events`가 들어간 job만 여기 나타남 (7-2의 최신 템플릿에는 이미 포함됨).
- **다시 설정해야 하는 경우**:
  - Windows 호스트를 재부팅했거나 터미널을 완전히 껐다 켠 경우 → 3번(port-forward)만 다시 실행
  - `docker compose down && up` 으로 `k8s-master-node`를 통째로 재생성한 경우 → k3s 자체가 초기화되므로 8-1 전체(RBAC, spark-events 디렉토리, History Server 배포)를 다시 해야 함
  - `docker restart k8s-master-node` (단순 재시작, 재생성 아님) → 컨테이너 내부 상태는 유지되므로 History Server Deployment는 자동으로 다시 Running이 됨. port-forward만 다시 걸어주면 됨.

---

## 9. 실시간 4040 — "지금 도는 job"을 매번 재설정 없이 보기

History Server(18080)는 event log를 주기적으로 읽어서 보여주는 방식이라 진짜 실시간이 아니고(수 초~수십 초 지연), progress bar 같은 순간순간의 진행률은 안 보임. **"지금 이 순간 작업이 돌아가고 있는지"를 실시간으로 보려면** 원래 방식대로 driver의 4040을 직접 봐야 하는데, 문제는 driver 서비스 이름이 매번 랜덤이라는 것.

### 실패한 시도: `kubectl proxy` + `spark.ui.reverseProxy`

`kubectl port-forward`는 시작 시점에 특정 팟 하나에 TCP 터널을 고정해버려서, 그 팟이 죽으면 끊기고 자동으로 다음 job의 팟에 재연결되지 않음. 이걸 해결하려고 `kubectl proxy`(요청마다 서비스 뒤 팟을 새로 찾아줌) + `spark.ui.reverseProxyUrl`(프록시 경로 뒤에서도 Spark UI 링크가 안 깨지게 하는 설정)를 시도했으나 **실패**:

```
'/api/v1/namespaces/default/services/spark-live-driver-ui:4040/proxy' in spark.ui.reverseProxyUrl is invalid.
Cannot use the keyword 'proxy' or 'history' in reverse proxy URL.
```

`kubectl proxy`의 URL 구조 자체가 반드시 `/proxy/`라는 경로를 포함하는데, Spark는 `reverseProxyUrl`에 "proxy"라는 단어가 들어가면 내부 REST API 경로 파싱과 충돌한다고 명시적으로 막아놓음. 두 방식이 구조적으로 호환되지 않음 (설정 실수가 아니라 근본적인 비호환).

### 최종 해법: 라벨 기반 고정 Service + `kubectl port-forward` 재시도 루프

핵심 깨달음: `reverseProxy` 설정은 URL에 **경로 접두사(prefix)가 붙는 경우**(kubectl proxy, Ingress 등)에만 필요함. `kubectl port-forward`는 접두사 없이 `localhost:4040`을 팟의 4040에 직결하기 때문에 애초에 `reverseProxy` 설정이 필요 없음. 남은 문제는 "매번 바뀌는 서비스 이름"뿐이고, 이건 Spark가 모든 driver 팟에 자동으로 붙이는 라벨 `spark-role=driver`를 이용해 해결.

1. **라벨 셀렉터 기반 고정 Service** (1회만 생성 — 이름이 절대 안 바뀜, 현재 떠있는 driver 팟이 누구든 자동으로 가리킴):
   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: spark-live-driver-ui
     namespace: default
   spec:
     selector:
       spark-role: driver
     ports:
       - port: 4040
         targetPort: 4040
   ```
   `kubectl apply -f live-driver-ui-svc.yaml`

2. **`kubectl port-forward` 자동 재시도 루프 스크립트** (port-forward는 연결된 팟이 죽으면 스스로 끊기므로, 끊길 때마다 자동으로 재연결):
   ```bash
   # ~/.kube_pysprak02/watch-4040.sh
   #!/bin/bash
   export KUBECONFIG="$HOME/.kube_pysprak02/k3s.yaml"
   while true; do
     kubectl port-forward svc/spark-live-driver-ui 4040:4040 >> "$HOME/.kube_pysprak02/live4040.log" 2>&1
     sleep 3
   done
   ```
   실행 (호스트에서, **1회만**):
   ```
   chmod +x ~/.kube_pysprak02/watch-4040.sh
   nohup ~/.kube_pysprak02/watch-4040.sh > /dev/null 2>&1 &
   ```
3. 브라우저에서 http://localhost:4040 접속. job이 없을 때는 연결 안 되고(루프가 3초마다 재시도), job이 시작되면 자동으로 그 driver에 붙어서 실시간 화면이 뜸.

**검증 완료**: 실제 job을 재제출해서 http://localhost:4040/jobs/ 응답의 링크(`href="/jobs/"`, `href="/stages/"` 등)가 전부 정상 절대경로로 나오는 것 확인함 — `reverseProxy` 설정 없이도 문제없이 작동.

**주의**:
- 스크립트를 중복 실행하면 안 됨 (포트 4040 충돌 에러 발생). `ps aux | grep watch-4040` 또는 `netstat -ano | grep 4040`으로 기존 프로세스 확인 후 실행할 것.
- 다시 설정해야 하는 경우는 History Server(8-2)와 동일한 기준: Windows 재부팅/터미널 종료 → 2번(nohup 재실행)만 다시. `docker compose down/up`으로 노드 재생성 → Service부터 다시 생성. `docker restart` → 자동 복구, port-forward만 재실행.
- 지금 이 두 가지(History Server 18080 + 실시간 4040)를 합치면: **평소엔 18080으로 지난 기록 확인, 뭔가 지금 도는지 궁금할 때는 4040으로 실시간 확인**, 둘 다 재설정 없이 상시 접근 가능.
