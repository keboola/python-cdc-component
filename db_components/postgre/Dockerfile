# to be executed from the root folder
FROM python:3.11-buster
ENV PYTHONIOENCODING utf-8
ENV PYTHONPATH /code:/code/db_components:/code/db_components/db_common:/code/db_components/debezium


# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y build-essential

# Install Java dependencies needed for Tabula
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

COPY . /code

# set switch that enables correct JVM memory allocation in containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -Xmx512m"

RUN pip install flake8

RUN pip install -r /code/db_components/postgre/requirements.txt

WORKDIR /code/db_components/postgre/src

CMD ["python", "-u", "component.py"]
