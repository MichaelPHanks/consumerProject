FROM python:3
COPY consumer.py consumer.py
RUN pip install boto3
CMD ["python3" ,"consumer.py" ,"cs5260-requests" ,"-sqs", "widgets", "-table"]
