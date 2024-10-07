import os
import pymysql
import pandas as pd
from pyspark.sql import SparkSession

def get_conn():
    conn = pymysql.connect(
        host=os.getenv("DB", "127.0.0.1"),
        user='master',
        password='1234',
        database='modeldb',
        port=int(os.getenv("DB_PORT", "53306")),
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

log_file_path = "logs"  # Replace with the path to your log file
log_df = spark.read.csv(log_file_path, header=True, inferSchema=True)

log_df.show()

try:
    conn = get_conn()
    
    # Step 3: Fetch data from the database
    with conn.cursor() as cursor:
        # Execute a query to get data from the source_table
        cursor.execute("SELECT * FROM comments")
        result = cursor.fetchall() 

    # Convert result to a pandas DataFrame
    pdf = pd.DataFrame(result)

    # Convert pandas DataFrame to PySpark DataFrame
    db_df = spark.createDataFrame(pdf)

    # Step 4: Show the content of the DataFrame
    db_df.show()

    # Step 5: Update NULL columns
    joined_df = db_df.join(log_df, on='num', how='inner')
    print("this is the joined df")
    joined_df.show()
    updated_df = joined_df \
        .withColumn("prediction_result", when(col("prediction_result").isNull(), col("log_prediction_result")).otherwise(col("prediction_result"))) \
        .withColumn("prediction_time", when(col("prediction_time").isNull(), col("log_prediction_time")).otherwise(col("prediction_time"))) \
        .withColumn("remark", when(col("remark").isNull(), col("log_remark")).otherwise(col("remark"))) \
        .withColumn("label", when(col("label").isNull(), col("log_label")).otherwise(col("label")))    

    # Step 7: Write the updated data back to the database
    # Convert updated_df back to pandas DataFrame
    updated_pdf = updated_df.toPandas()
    
    # Write back to the database using pymysql
    with conn.cursor() as cursor:
        for index, row in updated_pdf.iterrows():
            cursor.execute("""
                UPDATE comments 
                SET prediction_result = %s, prediction_time = %s, label = %s 
                WHERE id = %s
            """, (row['prediction_result'], row['prediction_time'], row['label']))
        conn.commit()  # Commit the changes

except Exception as e:
    print(f"An error occurred: {e}")

#finally:
    # Close the connection
 #   conn.close()
    # Stop the Spark session
  #  spark.stop()
