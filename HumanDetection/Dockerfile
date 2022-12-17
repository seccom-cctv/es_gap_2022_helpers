FROM python:3.9-slim

WORKDIR /human-detection-module

COPY requirements.txt requirements.txt
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6  -y
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt
COPY human-detection-module/ .
COPY .env-prod .env

CMD ["python3", "main.py"]