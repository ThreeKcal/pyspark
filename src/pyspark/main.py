from pyspark.sql import SparkSession
from threekcal_model.db.py import get_conn, select, dml

spark = SparkSession.builder.appName("LogToMariaDB").getOrCreate()
#Spark로 predict.log읽기
path='pj3/dags/predict.log'
log_df = spark.read.csv("path_to_log_file.csv", header=True, inferSchema=True)

#MariaDB로 연결하기
get_conn()

dml()

