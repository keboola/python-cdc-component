# to be executed from the root folder
FROM python:3.12-bookworm
ENV PYTHONIOENCODING utf-8
ENV PYTHONPATH /code:/code/db_components:/code/db_components/db_common:/code/db_components/debezium

ENV TMPDIR=/opt/tmpdir

RUN mkdir -p /opt/tmpdir

# Install required packages
RUN apt update && \
    apt install -y \
    build-essential \
    openjdk-17-jre-headless && \
    apt clean


# needs to be built from root folder
COPY /debezium_core /code/debezium_core/
COPY /db_components/db_common /code/db_components/db_common/
COPY /db_components/debezium /code/db_components/debezium/

COPY /db_components/ex_mysql_cdc /code/db_components/ex_mysql_cdc/

# Download the file from the URL and store it as 'kbc_slicer'
RUN wget -O /usr/local/bin/kbc_slicer https://github.com/keboola/processor-split-table/releases/download/v2.2.0/cli_linux_amd64
# Make 'kbc_slicer' executable
RUN chmod +x /usr/local/bin/kbc_slicer


# set switch that enables correct JVM memory allocation in containers
ENV JDK_JAVA_OPTIONS="-XX:+UseContainerSupport -Xmx1000m -XX:+HeapDumpOnOutOfMemoryError"

RUN pip install flake8

RUN pip install -r /code/db_components/ex_mysql_cdc/requirements.txt

WORKDIR /code/db_components/ex_mysql_cdc/src

CMD ["python", "-u", "component.py"]
