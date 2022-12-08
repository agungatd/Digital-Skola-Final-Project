from pyspark.sql import SparkSession

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
            url='jdbc:mysql://host.docker.internal:3306/digital_skola',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='home_credit_default_risk_application_train',
            user='root',
            password='Sukses37').load()

      application_test = spark.read.format('jdbc').options(
            url='jdbc:mysql://host.docker.internal:3306/digital_skola',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='home_credit_default_risk_application_test',
            user='root',
            password='Sukses37').load()

      # Load data to Postgresql
      application_train.write.format('jdbc').options(
            url='jdbc:postgresql://host.docker.internal:3306/postgres?ssl=false&sslMode=disable',
            driver='org.postgresql.Driver',
            dbtable='home_credit_default_risk_application_train',
            user='postgres',
            password='Sukses37').mode('overwrite').save()

      application_test.write.format('jdbc').options(
            url='jdbc:postgresql://host.docker.internal:3306/postgres?ssl=false&sslMode=disable',
            driver='org.postgresql.Driver',
            dbtable='home_credit_default_risk_application_test',
            user='postgres',
            password='Sukses37').mode('overwrite').save()
