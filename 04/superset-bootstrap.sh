#!/bin/bash
set -e

# trino 파이썬 클라이언트(trino.sqlalchemy)가 SQLAlchemy 다이얼렉트를 자체 내장하고 있어서
# 별도 sqlalchemy-trino 패키지는 불필요(PyPI의 그 이름은 사실상 빈 껍데기).
# 주의: 의존성 포함 설치 시 SQLAlchemy 2.x가 딸려올 수 있는데, 이러면 Superset 코어(venv에 고정된
# SQLAlchemy 1.4.x)가 깨짐(eagerload는 2.0에서 제거된 API) — trino 자체는 sqlalchemy를 요구하지 않아 안전함.
rm -rf /app/superset_home/.local
pip install --quiet --no-cache-dir trino

superset db upgrade
superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin || true
superset init

exec superset run -h 0.0.0.0 -p 8088
