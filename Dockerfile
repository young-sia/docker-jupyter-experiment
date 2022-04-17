FROM python:3.9.6

COPY requirements.txt .

RUN pip install -r requirements.txt && rm requirements.txt

COPY pipeline.py .

CMD ["python", "pipeline.py"]
