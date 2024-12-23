import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, StringType, IntegerType
import os
import subprocess
import sys
from datetime import datetime



if __name__ == "__main__":  

    # Initial path
    initial_path = '/data/'
    
    # Load proxy settings
    with open(f'{initial_path}airflow/dags/configs/proxies.json', 'r') as f:
        proxies = pd.read_json(f, typ='series') 

    proxy_url = proxies.iloc[0]
    ip_port = proxy_url.split('//')[1]
    ip, port = ip_port.split(':')
    print(f"Proxy IP: {ip}, Port: {port}")
    
    # Load S3 credentials
    cred_s3 = pd.read_json(f'{initial_path}airflow/dags/cred/s3_minio_cr.json', typ='series') 
    bucket_name, endpoint_url, region_name, access_key, secret_access_key = cred_s3
    
    # Set Google Application Credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{initial_path}airflow/dags/credentials/cred.json'
    credentials = service_account.Credentials.from_service_account_file(f'{initial_path}airflow/dags/credentials/cred.json')
    project_id = 'project'
    clientBQ = bigquery.Client(credentials=credentials, project=project_id)

  
    # Start Spark session
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .master("local[10]") \
        .config("spark.jars", f"{initial_path}airflow/spark_connectors/spark-3.5-bigquery-0.40.0.jar, {initial_path}airflow/spark_connectors/aws-java-sdk-bundle-1.11.901.jar, {initial_path}airflow/spark_connectors/hadoop-aws-3.3.1.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", f"{initial_path}airflow/dags/credentials/cred.json") \
        .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", f"{initial_path}airflow/dags/credentials/cred.json") \
        .config("spark.sql.debug.maxToStringFields", "500") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.driver.cores", "8") \
        .config("spark.executor.cores", "8") \
        .config("spark.driver.extraJavaOptions", f"-Dhttp.proxyHost={ip} -Dhttp.proxyPort={port} -Dhttps.proxyHost={ip} -Dhttps.proxyPort={port}") \
        .config("spark.executor.extraJavaOptions", f"-Dhttp.proxyHost={ip} -Dhttp.proxyPort={port} -Dhttps.proxyHost={ip} -Dhttps.proxyPort={port}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
        .config("spark.hadoop.fs.s3a.bucket.name", bucket_name) \
        .config("spark.hadoop.fs.s3a.region", region_name) \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    print("Spark session successfully started.")

    # Read data from Google BigQuery
    columns = [
        "date", "articlenumber", "item_url", ...]


    # Read data from Google BigQuery
    df = spark.read \
      .format("bigquery") \
      .option("selectedFields", ",".join(columns)) \
      .load("project.special_projects.mpstats_test6")
    

    # Rename columns
    rename_columns = {
        "date": "date",
        "articlenumber": "articlenumber",
        "item_url": "item_url",
      ...
    }


    for old_name, new_name in rename_columns.items():
        df = df.withColumnRenamed(old_name, new_name)

    # Apply dtypes to columns
    df = df.withColumn("date", col("date").cast(StringType())) \
           .withColumn("articlenumber", col("articlenumber").cast(StringType())) \
           .withColumn("item_url", col("item_url").cast(StringType())) \
          ...
    
    # Save DataFrame as a single Parquet file to S3
    s3_path = f"s3a://{bucket_name}/megamarket-data/{datetime.now().strftime('%Y-%m-%d')}/"
    df.repartition(1).write.mode("overwrite").parquet(s3_path)
    
    print(f"Data written to S3 at {s3_path}")

