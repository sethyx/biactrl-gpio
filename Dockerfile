FROM --platform=linux/arm64 python:3.11-slim-bookworm

RUN apt-get update && \
    apt-get install -y gcc

RUN pip install lgpio RPi.GPIO gpiozero schedule

COPY . .

CMD python -u qmonitor.py