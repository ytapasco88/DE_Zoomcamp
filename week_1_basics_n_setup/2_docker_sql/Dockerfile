FROM python:3.9

RUN pip install pandas numpy

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]