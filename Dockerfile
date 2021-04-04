FROM python:3.9

WORKDIR /code

ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

RUN apt-get update -y && apt-get install -y build-essential
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN curl -o oapi-client.zip -s -X POST \
  https://generator3.swagger.io/api/generate \
  -H 'content-type: application/json' \
  -d '{"specURL": "https://raw.githubusercontent.com/treeverse/lakeFS/master/api/swagger.yml", "lang": "python", "type": "CLIENT", "codegenVersion": "V3"}'
RUN unzip oapi-client.zip -d oapi-client
RUN pip install oapi-client/

EXPOSE 5000
COPY . .
CMD ["flask", "run"]
