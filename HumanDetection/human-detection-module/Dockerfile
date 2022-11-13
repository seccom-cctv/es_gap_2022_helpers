FROM python:3.8-slim

WORKDIR /

COPY /requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install opencv-python-headless
COPY . .

WORKDIR /human-detection-module

CMD ["python3", "main.py"]