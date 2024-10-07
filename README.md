# pyspark

![pyspark_proj](https://github.com/user-attachments/assets/c678225e-e5c0-4da0-9cac-b8025b5a8a74)

## Dependencies
- [ ] pyspark
- [ ] threekcal_model.db

# 기능
```python
spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()

#Spark로 predict.log읽기
log_file='predict.log'
log_load = spark.read.csv("predict.log", header=True)
```
SparkSession을 열어서 predict.log파일을 읽은다

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
threekcal.model.db.py에 있는 get_conn()을 사용하여 MariaDB로 연결한다

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
result라는 table을 많들고 서버에 있는 DB와 logDB를 합치면서 NULL값 처리

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
마지막으로 MariaDB서버에 있는 테이블을 없데이트한다.
