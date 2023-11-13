FROM python:3.9-slim-buster
RUN apt-get update
RUN pip install pandas
RUN pip install mysql-connector-python
RUN pip install SQLAlchemy  
RUN pip install pymongo
RUN pip install kafka-python
RUN pip install requests
Run pip install schedule
RUN pip install protobuf==3.20.*
RUN pip install shared-memory-dict
RUN pip install PyYaml
Run pip install "fastapi[all]"
Run pip install minio
copy summarization /app
WORKDIR /app
# RUN mkdir /app/logs
cmd ["python3","app.py"]