FROM python:3.9-slim

WORKDIR /app

RUN pip install gremlinpython

COPY . /app

CMD ["python", "benchmark.py"]
