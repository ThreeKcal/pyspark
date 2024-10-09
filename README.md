# pyspark
## Overview
ML 어플리케이션 서비스 중 `pyspark` 코드를 위한 리포지토리

팀 프로젝트 #3: 팀 ThreeKcal

`DistilRoBERTa` 기반의 text classifier 모델인 [michellejieli/emotion_text_classifier](https://huggingface.co/michellejieli/emotion_text_classifier) 을 통해:
- `Streamlit` 기반 웹 어플리케이션을 통해 사용자 입력을 받고, 해당 문장에 대한 sentiment analysis/prediction 실행 (🤬🤢😀😐😭😲)
- 해당 prediction에 대해 실제 sentiment label 및 피드백 코멘트 역시 입력
- Model 부분을 더 알고 싶다면 [이 리포지토리](https://github.com/ThreeKcal/model/tree/main) 확인
- Airflow 부분을 더 알고 싶다면 [이 리포지토리](https://github.com/ThreeKcal/dags/tree/main)  확인


## Features
![pyspark_proj](https://github.com/user-attachments/assets/c678225e-e5c0-4da0-9cac-b8025b5a8a74)

본 리포지토리 코드의 목적은 `airflow`로 저장된 `predict.log` 파일을 읽어서 `MariaDB`에 있는 테이블을 업데이트하는 데 있습니다. 

## Code specifics
### Dependencies
- [ ] pyspark
- [ ] threekcal_model.db

### Python
```python
spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()

#Spark로 predict.log읽기
log_file='predict.log'
log_load = spark.read.csv("predict.log", header=True)
```
SparkSession을 열어서 predict.log파일을 읽어

```python
def connection():
    conn = get_conn()

    with conn:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM comments"
            cursor.execute(sql)
            result = cursor.fetchall()

    return result

data = connection()
df = pd.DataFrame(data)
```
threekcal.model.db.py에 있는 get_conn()을 사용하여 MariaDB로 연결합니다.

```python
result = spark.sql(f"""
SELECT db.num as num,
        db.comments as comments,
        db.request_time as request_time,
        db.request_user as request_user,
        log.prediction_result as prediction_result,
        log.prediction_score as prediction_score,
        log.prediction_time as prediction_time
FROM db_table db
JOIN log_table log ON db.num = log.num""")

df1 = result.toPandas()
```
이후 result라는 table을 만들고 서버에 있는 DB와 logDB를 합치면서 NULL값 처리해,

```python
def get_prediction():
    conn = get_conn()
    with conn:
        with conn.cursor() as cursor:
            #zip 사용은 추가 공부가 필요
            params = list(zip(df1['prediction_result'], df1['prediction_score'], df1['prediction_time'], df1['num']))
            cursor.executemany("""UPDATE comments
            SET prediction_result=%s,
                prediction_score=%s,
                prediction_time=%s
            WHERE num=%s
            """, params)

            # 변경 사항을 커밋
            conn.commit()

        # 데이터 확인을 위해 SELECT 실행
        with conn.cursor() as cursor:
            sql_select = "SELECT * FROM comments"
            cursor.execute(sql_select)
            prediction = cursor.fetchall()

```
마지막으로 MariaDB서버에 있는 테이블을 업데이트하게 됩니다.


## 개발 관련 사항
### 타임라인
![스크린샷 2024-10-10 010952](https://github.com/user-attachments/assets/7bed00cb-272e-49e1-83f4-3986dd6bfcff)

※ 권한이 있는 이용자는 [프로젝트 schedule](https://github.com/orgs/ThreeKcal/projects/1/views/4)에서 확인할 수 있습니다.

### troubleshooting
- 본 리포지토리 및 연관 리포지토리들의 `issues`, `pull request` 쪽을 참조해 주세요.
