from pyspark.sql import SparkSession

if __name__ == '__main__':
    
    # initiate  spark
    spark = SparkSession \
        .builder \
        .config("spark.jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.30.jar") \
        .master("local") \
        .appName("ds9_final_project") \
        .getOrCreate()
        
    # Read data from csv
    application_test = spark.read. \
        format("csv"). \
        option("inferSchema","true"). \
        option("header","true"). \
        load("/usr/local/spark/resources/application_test.csv")

    application_train = spark.read. \
        format("csv"). \
        option("inferSchema","true"). \
        option("header","true"). \
        load("/usr/local/spark/resources/application_train.csv")

    # Load data to MySQL
    application_train.write.format('jdbc').options(
        #   url='jdbc:mysql://host.docker.internal:3306/digitalSkola',
        url='jdbc:mysql://host.docker.internal:3306/digital_skola',
        driver='com.mysql.jdbc.Driver',
        dbtable='home_credit_default_risk_application_train',
        user='root',
        password='Sukses37').mode('overwrite').save()

    application_test.write.format('jdbc').options(
        url='jdbc:mysql://host.docker.internal:3306/digital_skola',
        driver='com.mysql.jdbc.Driver',
        dbtable='home_credit_default_risk_application_test',
        user='root',
        password='Sukses37').mode('overwrite').save()
