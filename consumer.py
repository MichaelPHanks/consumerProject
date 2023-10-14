# Loop until some stop condition is met:
import json
import sys
import boto3
import logging
import time

def main():
    bucket2 = sys.argv[1]
    destination = sys.argv[2]
    isTable = False
    if sys.argv[3] == '-table':
        isTable = True
    print(bucket2)
    print(destination)


    consumer(bucket2, destination, isTable)


def consumer(bucket2, destination, isTable):
    logging.basicConfig(filename="consumer.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3")
    startTime = time.time()
    endTime = time.time()
    count = 0
    while endTime - startTime < 50:

        try:
            request = s3.list_objects_v2(
                Bucket=bucket2,
                MaxKeys=1,

            )

            if request['KeyCount'] == 1:
                startTime = time.time()
                endTime = time.time()

            #if request['KeyCount'] == 1:
                for i in range(request['KeyCount']):
                    # Delete the object...
                    key = request['Contents'][i]['Key']
                    widget = s3.get_object(Bucket=bucket2, Key=request['Contents'][i]['Key'])

                    widget_request_str = widget['Body'].read().decode('utf-8')
                    widget_request = json.loads(widget_request_str)
                    #print(widget_request)
                    if widget_request['type'] == 'create':
                        if isTable:
                            print("Copying to specified DynamoDB table...")
                        else:
                            print("Copying to specified s3 bucket...")
                            object_key = 'widgets/' + widget_request['owner'] + '/' + widget_request['widgetId']
                            s3.put_object(Bucket=destination, Key=object_key)
                            #print(object_key)
                            count +=1
                            logger.info("Created new object " + widget_request['widgetId'])

                        ## write it into the new table
                    if widget_request['type'] == 'update':
                        logger.info("'Updated' (did not actually happen) object "+ widget_request['widgetId'])

                    if widget_request['type'] == 'delete':
                        logger.info("'Deleted' (not actually deleted) object "+ widget_request['widgetId'])


                    deleteResponse = s3.delete_object(Bucket=bucket2, Key=key)

            else:
                endTime = time.time()
                time.sleep(0.1)


        except Exception as e:
            print(e)

    print("Count is : ",count)
    if endTime - startTime >= 50:
        print("Program exited: no widgets found in last 50 seconds in bucket ", bucket2)





if len(sys.argv) != 4:
    print("Incorrect number of arguments, structure should be: python3 {name of requests bucket} {name of bucket 3 or DynamoDB table}")
else:
    main()

