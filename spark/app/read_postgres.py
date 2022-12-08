from pyspark.sql import SparkSession
import logging

if __name__ == '__main__':
    # initiate  spark

    spark = SparkSession \
        .builder \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.25.jar, /opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .master("local") \
        .appName("ds9_final_project") \
        .getOrCreate()
        
    # Read data from mySQL
    application_train = spark.read.format('jdbc').options(
        url='jdbc:postgresql://host.docker.internal:3306/postgres?ssl=false&sslMode=disable',
        driver='org.postgresql.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='root',
        password='Sukses37').load()

    application_test = spark.read.format('jdbc').options(
        url='jdbc:postgresql://host.docker.internal:3306/postgres?ssl=false&sslMode=disable',
        driver='org.postgresql.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='root',
        password='Sukses37').load()

    logging.info(f"TRAIN: {application_train.show(1)}")
    logging.info(f"TEST: {application_test.show(1)}")
    
