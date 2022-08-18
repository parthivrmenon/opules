import os
import boto3 
import json
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS


def parseEvent(event, defaultBucket):
    try:
        bucket = event.get("bucket", defaultBucket)
        key = event["key"]
        return bucket,key

    except Exception as err:
        raise Exception(f"Could not parse input, {err}")

def deleteFromS3(bucket, key):
    s3_client = boto3.client('s3')
    s3_client.delete_object(Bucket=bucket, Key=key)
    

def getFromS3(bucket, key):
    try:
        s3_client = boto3.client('s3')
        s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
        object_content = s3_response_object['Body'].read().decode('utf-8') 
        object_json = json.loads(object_content)
        return object_json
    except Exception as err:
        raise Exception(f"Could not get data from S3. {err}")

def writeToInfluxDb(data, influxUrl, influxToken, influxOrg, influxBucket):

    try:
        client = influxdb_client.InfluxDBClient(
        url=influxUrl,
        token=influxToken,
        org=influxOrg,
        debug=True,
        timeout=15000
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        measurement = data["name"]
        for row in data["measurements"]:
            timestamp = row["timestamp"]
            metric = row["metric"]
            metric_value = row["metric_value"]
            p = Point(measurement).field(metric, metric_value).time(timestamp)
            #for tag in row["tags"]:
                #for tag_name, tag_value in tag.items():
                    #p.tag(tag_name, tag_value)
            response = write_api.write(bucket=influxBucket, org=influxOrg, record=p)
            print(f"Wrote {row}, {response}")

    except Exception as err:
        raise Exception(f"Could not write to Influx DB, {err}")        


def opuleHandler(event, context):
    try:
        OPULENCE_S3 = os.environ["OPULENCE_S3"]
        OPULENCE_DB_URL = os.environ["OPULENCE_DB_URL"]
        OPULENCE_DB_TOKEN = os.environ["OPULENCE_DB_TOKEN"]
        OPULENCE_DB_ORG = os.environ["OPULENCE_DB_ORG"]
        OPULENCE_DB_BUCKET = os.environ["OPULENCE_DB_BUCKET"]
        bucketName, fileName = parseEvent(event, defaultBucket=OPULENCE_S3)
        data = getFromS3(bucketName, fileName)
        writeToInfluxDb(data,OPULENCE_DB_URL,OPULENCE_DB_TOKEN,OPULENCE_DB_ORG, OPULENCE_DB_BUCKET)
        #deleteFromS3(bucketName, fileName)
        return {
            'statusCode': 200,
            'status': 'success',
            'errors':""
        }


    except Exception as err:
        return {
            'statusCode': 400,
            'status': 'error',
            'errors': str(err)
        }