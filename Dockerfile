FROM python:3.14
COPY requirements.txt
COPY main.py
COPY config.py
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "main.py"]
