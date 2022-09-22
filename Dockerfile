FROM python:3.9

WORKDIR /code
ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

RUN apt-get update -y && apt-get install -y build-essential ca-certificates
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . ./

EXPOSE 5000

CMD ["flask", "run"]
