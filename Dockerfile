FROM python:3.13.2-slim


WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY pytes_serial.py .

ENTRYPOINT ["python3", "pytes_serial.py"]
