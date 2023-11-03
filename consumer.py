# Loop until some stop condition is met:
import json
import sys
import boto3
import logging
import time

def main():
    bucket2 = sys.argv[1]
    destination = sys.argv[3]
    isTable = False
    isSQS = False
    if sys.argv[4] == '-table':
        isTable = True
    if sys.argv[2] == '-sqs':
        isSQS = True

    session = boto3.Session()
    if session.get_credentials() is None:
        print("AWS credentials are not available.")
    else:
        consumer(bucket2, destination, isTable, isSQS)


def consumer(bucket2, destination, isTable, isSQS):
    count = 0
    logging.basicConfig(filename="consumer.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3", region_name='us-east-1')
    sqs = boto3.client('sqs', region_name='us-east-1')
    database = boto3.resource("dynamodb", region_name='us-east-1')
    startTime = time.time()
    endTime = time.time()
    while endTime - startTime < 50:

        try:
            if not isSQS:
                endTime, startTime = grabFromBucket(bucket2, destination, isTable, s3, endTime, database, logger, startTime)
            else:
                endTime, startTime, count = grabFromSQS(bucket2, destination, isTable, sqs, s3,  endTime, database, logger, startTime, count)


        except Exception as e:
            print("There was an error: ", e)

    if endTime - startTime >= 50:
        print("Program exited: no widgets found in last 50 seconds in bucket/sqs", bucket2)
        print(count)

def grabFromSQS(queue, destination, isTable, sqs, s3, endTime, database, logger, startTime, count):
    #request = sqs.list_queues()
    response = sqs.receive_message(
        QueueUrl=queue,
        MaxNumberOfMessages=10,
    )
    if 'Messages' in response:
        if len(response['Messages']) > 0:
            startTime = time.time()
            endTime = time.time()
            for widget in response['Messages']:
                count +=1
                messageId = widget['MessageId']
                reciept = widget['ReceiptHandle']
                body = json.loads(widget['Body'])

                

                if body["type"] == "create":

                    if isTable:


                        print("Creating widget " + body['widgetId']+", and placing in specified DynamoDB table...")
                        table = database.Table(destination)
                        body['id'] = body['widgetId']
                        body = parseRequest(body)

                        table.put_item(Item=body)
                        logger.info("Created new object " + body['widgetId'] + " and placed it into dynamodb table " + destination)
                    else:

                        print("Creating widget " + body['widgetId']+", and placing in specified s3 bucket...")
                        object_key = 'widgets/' + convertOwner(body['owner']) + '/' + body['widgetId']
                        s3.put_object(Bucket=destination, Key=object_key, Body=json.dumps(body))
                        logger.info("Created new object " + body['widgetId'] + " and placed it into s3 bucket " + destination)
            




                if body['type'] == 'update':
                    if isTable:
                        table = database.Table(destination)
                        oldRequest = table.get_item(
                            Key={'id': body['widgetId']}
                        )
                        if oldRequest:
                            body['id'] = body['widgetId']
                            if 'Item' in oldRequest:
                                newWidget = updateWidget(oldRequest['Item'], parseRequest(body))
                                table.delete_item(
                                    Key={'id': body['widgetId']}

                                )
                                table.put_item(Item=newWidget)
                                logger.info("Updated object "+ body['widgetId'])
                                print("Updated object " + body['widgetId'])

                        else:
                            logger.warning('widgets/' + convertOwner(body['owner']) + '/' + body['widgetId'] + " does not exist...")

                    else:
                        oldRequest = s3.get_object(
                            Bucket=destination,
                            Key='widgets/' + convertOwner(body['owner']) + '/' + body['widgetId']
                        )

                        if oldRequest:
                            oldRequest = oldRequest['Body'].read().decode('utf-8')
                            oldRequest = json.loads(oldRequest)
                            newWidget = updateWidget(parseRequest(oldRequest), parseRequest(body))
                            s3.delete_object(
                                Bucket=destination,
                                Key='widgets/' + convertOwner(body['owner']) + '/' + body['widgetId']
                            )
                            s3.put_object(Bucket=destination,Key='widgets/' + convertOwner(newWidget['owner']) + '/' + newWidget['widgetId'], Body=json.dumps(newWidget) )
                            logger.info("Updated object "+ body['widgetId'])
                            print("Updated object " + body['widgetId'])
                        else:
                            logger.warning('widgets/' + convertOwner(body['owner']) + '/' + body['widgetId'] + " does not exist...")



                if body['type'] == 'delete':
                    if isTable:
                        table = database.Table(destination)

                        item = table.get_item(
                            Key={'id': body['widgetId']}
                        )
                        if item:
                            table.delete_item(
                                Key={'id': body['widgetId']}

                            )
                            logger.info("Deleted key "  + body['widgetId'])
                            print("Deleted object " + body['widgetId'])
                        else:
                            logger.warning('widgets/' + convertOwner(body['owner']) + '/' + body['widgetId'] + " does not exist...")


                    else:
                        item = s3.get_object(
                            Bucket=destination,
                            Key='widgets/' + convertOwner(body['owner']) + '/' + body['widgetId']
                        )
                        if item:
                            s3.delete_object(
                                Bucket=destination,
                                Key='widgets/' + convertOwner(body['owner']) + '/' + body['widgetId']
                            )
                            logger.info("Deleted key "  + body['widgetId'])
                            print("Deleted object " + body['widgetId'])

                        else:
                            logger.warning('widgets/' + convertOwner(body['owner']) + '/' + body['widgetId'] + " does not exist...")
                sqs.delete_message(
                    QueueUrl=queue,
                    ReceiptHandle=reciept
                   
                )
        
    else:
        endTime = time.time()
        time.sleep(0.1)
    

    return endTime, startTime, count











def grabFromBucket(bucket2, destination, isTable, s3, endTime, database, logger, startTime):
    request = s3.list_objects_v2(
                Bucket=bucket2,
                MaxKeys=1,

            )

    if request['KeyCount'] == 1:
        startTime = time.time()
        endTime = time.time()
        for i in range(request['KeyCount']):
            key = request['Contents'][i]['Key']
            ##Grabbing top object
            widget = s3.get_object(Bucket=bucket2, Key=key)
            s3.delete_object(Bucket=bucket2, Key=key)


            ##Converting to python object
            widget_request_str = widget['Body'].read().decode('utf-8')
            widget_request = json.loads(widget_request_str)
            if widget_request['type'] == 'create':
                if isTable:
                    print("Creating widget " + widget_request['widgetId']+", and placing in specified DynamoDB table...")
                    widget_request['id'] = widget_request['widgetId']
                    table = database.Table(destination)

                    widget_request = parseRequest(widget_request)

                    table.put_item(Item=widget_request)
                    logger.info("Created new object " + widget_request['widgetId'] + " and place it into dynamodb table " + destination)
                else:
                    print("Creating widget " + widget_request['widgetId']+", and placing in specified s3 bucket...")
                    object_key = 'widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId']
                    s3.put_object(Bucket=destination, Key=object_key, Body=json.dumps(widget_request))
                    logger.info("Created new object " + widget_request['widgetId'] + " and placed it into s3 bucket " + destination)

            if widget_request['type'] == 'update':
                if isTable:
                    table = database.Table(destination)

                    oldRequest = table.get_item(
                        Key={'id': widget_request['widgetId']}
                    )
                    if oldRequest:
                        widget_request['id'] = widget_request['widgetId']
                        newWidget = updateWidget(oldRequest['Item'], parseRequest(widget_request))
                        table.delete_item(
                            Key={'id': widget_request['widgetId']}

                        )
                        table.put_item(Item=newWidget)
                        logger.info("Updated object "+ widget_request['widgetId'])
                        print("Updated object " + widget_request['widgetId'])

                    else:
                        logger.warning('widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId'] + " does not exist...")

                else:
                    oldRequest = s3.get_object(
                        Bucket=destination,
                        Key='widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId']
                    )
                    if oldRequest:
                        oldRequest = oldRequest['Body'].read().decode('utf-8')
                        oldRequest = json.loads(oldRequest)
                        newWidget = updateWidget(parseRequest(oldRequest), parseRequest(widget_request))
                        s3.delete_object(
                            Bucket=destination,
                            Key='widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId']
                        )
                        s3.put_object(Bucket=destination,Key='widgets/' + convertOwner(newWidget['owner']) + '/' + newWidget['widgetId'], Body=json.dumps(newWidget) )
                        logger.info("Updated object "+ widget_request['widgetId'])
                        print("Updated object " + widget_request['widgetId'])
                    else:
                        logger.warning('widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId'] + " does not exist...")



            if widget_request['type'] == 'delete':
                if isTable:
                    table = database.Table(destination)

                    item = table.get_item(
                        Key={'id': widget_request['widgetId']}
                    )
                    if item:
                        table.delete_item(
                            Key={'id': widget_request['widgetId']}

                        )
                        logger.info("Deleted key "  + widget_request['widgetId'])
                        print("Deleted object " + widget_request['widgetId'])
                    else:
                        logger.warning('widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId'] + " does not exist...")


                else:
                    item = s3.get_object(
                        Bucket=destination,
                        Key='widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId']
                    )
                    if item:
                        s3.delete_object(
                            Bucket=destination,
                            Key='widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId']
                        )
                        logger.info("Deleted key "  + widget_request['widgetId'])
                        print("Deleted object " + widget_request['widgetId'])

                    else:
                        logger.warning('widgets/' + convertOwner(widget_request['owner']) + '/' + widget_request['widgetId'] + " does not exist...")

                       







            # Delete the object...
    else:
        endTime = time.time()
        time.sleep(0.1)
    return endTime, startTime




def convertOwner(name):
    name = name.lower()
    name = name.replace(" ", "-")


    return name

def parseRequest(request):
    if 'otherAttributes' in request:
        for i in range(len(request['otherAttributes'])):
            newName = request['otherAttributes'][i]['name']
            newValue = request['otherAttributes'][i]['value']
            request[newName] =newValue

        request.pop('otherAttributes')
    return request

def updateWidget(oldRequest, newRequest):
    updatedRequest = oldRequest
    for item in newRequest:
        if item in updatedRequest and item != 'id' and item != 'owner':
            if newRequest[item] == '':
                updatedRequest.pop(item)
            else:
                updatedRequest[item] = newRequest[item]
        elif item not in updatedRequest and item != 'widgetId' and item != 'owner':
            updatedRequest[item] = newRequest[item]

    return updatedRequest
    




if len(sys.argv) != 5:
    print("Incorrect number of arguments, structure should be: python3 {name of requests bucket} {name of bucket 3 or DynamoDB table}")
else:
    main()

