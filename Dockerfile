FROM python:3
COPY consumer.py consumer.py
RUN pip install boto3
CMD ["python3" ,"consumer.py" ,"cs5260-requests" ,"-sqs", "widgets", "-table"]
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 675489061989.dkr.ecr.us-east-1.amazonaws.com
docker tag c10c0a5630d2 675489061989.dkr.ecr.us-west-2.amazonaws.com/cs5260
