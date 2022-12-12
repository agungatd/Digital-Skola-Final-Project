from pyspark.sql import SparkSession

if __name__ == '__main__':
      # initiate  spark
      spark = SparkSession \
            .builder \
            .config("spark.jars", "/usr/local/spark/resources/mysql-connector-java-8.0.30.jar") \
            .master("local") \
            .appName("ds9_final_project") \
            .getOrCreate()

      # Read data from mySQL
      application_train = spark.read.format('jdbc').options(
            url='jdbc:mysql://localhost:3306/digital_skola',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='home_credit_default_risk_application_train',
            user='root',
            password='password123').load()

      application_test = spark.read.format('jdbc').options(
            url='jdbc:mysql://localhost:3306/digital_skola',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='home_credit_default_risk_application_test',
            user='root',
            password='password123').load()

      spark = SparkSession \
            .builder \
            .config("spark.jars", "/usr/local/spark/resources/postgresql-42.2.25.jar") \
            .master("local") \
            .appName("ds9_final_project") \
            .getOrCreate()

      # Load data to Postgresql
      application_train.write.format('jdbc').options(
            url='jdbc:postgresql://localhost:3306/postgres',
            driver='org.postgresql.Driver',
            dbtable='home_credit_default_risk_application_train',
            user='postgres',
            password='password123').mode('overwrite').save()

      application_test.write.format('jdbc').options(
            url='jdbc:postgresql://localhost:3306/postgres',
            driver='org.postgresql.Driver',
            dbtable='home_credit_default_risk_application_test',
            user='postgres',
            password='password123').mode('overwrite').save()
