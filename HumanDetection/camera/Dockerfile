FROM python:3.8

WORKDIR /

COPY /requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6  -y
COPY . .

WORKDIR /camera

CMD ["python3", "main.py"]