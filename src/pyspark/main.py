from pyspark.sql import SparkSession
from threekcal_model.db import get_conn, select, dml
import pandas as pd
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType, StructField

spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()

# 스키마 정의
schema = StructType([
    StructField("num", IntegerType(), True),
    StructField("prediction_result", StringType(), True),
    StructField("prediction_time", StringType(), True)
])

#Spark로 predict.log읽기
log_file='predict.log'
log_load = spark.read.csv("predict.log", header=True, schema=schema)

#MariaDB로 연결하기

def connection():
    conn = get_conn()

    with conn:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM comments"
            cursor.execute(sql)
            result = cursor.fetchall()
    
    return result
    
def to_spark_df():
    data = connection()

    #결과를 데이터프레임으로 변경
    df = pd.DataFrame(data)

    # 스키마 정의
    schema = StructType([
        StructField("num", IntegerType(), True),
        StructField("comments", StringType(), True),
        StructField("request_time", StringType(), True),
        StructField("request_user", StringType(), True)
    ]) 
    spark_df = spark.createDataFrame(df, schema=schema)

    return spark_df

comments_df = to_spark_df()

# 두 DataFrame 조인
joined_df = comments_df.join(log_load, comments_df["num"] == log_load["num"], "inner")
joined_df = joined_df.drop(log_load.num)
#joined_df = log_load.join(comments_df, log_load["num"] == comments_df["num"], "inner")

# 결과 출력
joined_df.show()
