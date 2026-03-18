# spark-pipeline
🚌 서울시 공공데이터를 이용하여 버스 운행 정보를 수집하고 Spark를 이용하여 최종적으로 BigQuery에 테이블 형태로 적재하는 파이프라인 입니다.

[Project Notion Documentation](https://www.notion.so/Spark-Pipeline-313103c0763380bbb113cba515c083b9?source=copy_link)

## 🛠️ 주요 기술 및 버전
```plaintext
python: 3.12.3 (GCP Compute Engine VM)
spark: 3.5.8
scala: 2.12.18
jdk: 17.0.18
apache-airflow: 3.1.7 (Docker)
```

## 🚀 Data Pipeline Flow
1. **서울시 공공데이터를 API를 이용하여 수집**
- [서울 열린데이터 광장](https://data.seoul.go.kr/)
    - **월간**
        - 서울시 노선 정류장마스터 정보
        - 서울시 정류장마스터 정보
        - 서울시 노선마스터 정보
    - **일간**
        - 행정동 단위 서울 생활인구(내국인)
        - 서울시 버스노선별 정류장별 승하차 인원 정보
        - 서울시 노선별 정류장별 총 버스 운행횟수 정보
        - 서울시 행정동별 버스 총 승차 승객수 정보
    - **+ 행정동 경계 데이터**
        - https://github.com/vuski/admdongkor
2. **Raw Data를 .json 형태로 적재 (GCS)**
3. **전처리 및 .parquet 형태로 적재 (GCS)**
4. **Star Schema(Fact/Dimension) 형태로 BigQuery에 적재**
5. **Airflow를 이용한 전체 Spark Cluster 운영 및 파이프라인 자동화**

## 📂 Project Structure
```plaintext
├── airflow_dags/
├── imgs/
├── python_scripts/
├── spark_scripts/
├── tests/             # pytest Scripts
└── requirements.txt   # pytest를 위해 가상 환경에서 필요한 라이브러리
```

## 👀 Key Features
- **Spark Cluster**: 1Driver - 3Worker 형태로 GCP에서 구성
- **Testing**: Pytest를 이용하여 Parquet 파일의 스키마 및 필수 컬럼 데이터 유효성 검사

## ⛔ 추가 사항
1. 행정동 경계 SHP 파일을 이용하여 위-경도 변환 및 기존 데이터와 매칭
2. 행정동 관련 미사용 데이터 BigQuery 테이블 생성 프로세스 추가
3. BigQuery 테이블 로드 -> Join 및 집계 테이블 생성 프로세스 추가
4. ML Pipeline? -> 예측할 내용 및 컬럼 생성 작업 필요. 이전 작업에서 최종 테이블로 만들어도 됨.
