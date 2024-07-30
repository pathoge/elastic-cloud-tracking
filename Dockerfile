FROM python:3.12

WORKDIR /app

COPY requirements.txt .
COPY cloud-usage.py .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "cloud-usage.py", "--debug", "--reset"]