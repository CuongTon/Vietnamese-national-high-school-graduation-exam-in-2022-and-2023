FROM apache/airflow:2.7.3

# to show spark in connection UI
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.4.0

# install pymongo
RUN pip install pymongo && \
    pip install Scrapy && \
    pip install --upgrade pip && \
    python -m pip install --upgrade pip

USER root
# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# install vim and enable test connection
RUN apt-get install -y vim
RUN export AIRFLOW__CORE__TEST_CONNECTION=Enabled

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

