from pyspark.sql import SparkSession
from threekcal_model.db import get_conn, select, dml
import pandas as pd
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType, StructField

spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()

#Spark로 predict.log읽기
log_file='predict.log'
log_load = spark.read.csv("predict.log", header=True)

#MariaDB로 연결하기

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

# 스키마 정의
schema = StructType([
        StructField("num", IntegerType(), True),
        StructField("comments", StringType(), True),
        StructField("request_time", StringType(), True),
        StructField("request_user", StringType(), True)
    ])

spark_df = spark.createDataFrame(df, schema=schema)

#테이블 생성
log_load.createOrReplaceTempView("log_table")
spark_df.createOrReplaceTempView("db_table")

#각 테이블을 조인
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

            # 결과 출력
            print(prediction)

get_prediction()
spark.stop()
