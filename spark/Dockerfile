FROM docker.io/bitnami/spark:3.2

USER root

RUN apt-get update && apt-get install -y wget

RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar -P /opt/bitnami/spark/jars

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.25.jar -P /opt/bitnami/spark/jars
