FROM python:3.9

WORKDIR /code
ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

RUN apt-get update -y && apt-get install -y build-essential
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000
COPY . .
CMD ["flask", "run"]
