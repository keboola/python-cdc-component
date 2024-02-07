# to be executed from the root folder
FROM python:3.11-buster
ENV PYTHONIOENCODING utf-8
ENV PYTHONPATH /code:/code/db_components:/code/db_components/db_common:/code/db_components/debezium

# Install required packages
RUN apt update && \
    apt install -y \
    build-essential \
    openjdk-11-jre-headless && \
    apt clean

COPY . /code

# set switch that enables correct JVM memory allocation in containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -Xmx512m"

RUN pip install flake8

RUN pip install -r /code/db_components/postgre/requirements.txt

WORKDIR /code/db_components/postgre/src

CMD ["python", "-u", "component.py"]
