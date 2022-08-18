import json
import os
import datetime
import boto3

AWS_DATE_FORMAT = '%Y-%m-%d'


def getCosts(start, end, granularity, metrics, groupBy):
    print(f'Getting {granularity} {metrics} costs from AWS from {start} to {end} for groups {groupBy}')
    ce_client = boto3.client("ce")
    time_period = {
         'Start': start,
         'End': end
     }
    results = []
    response = ce_client.get_cost_and_usage(
         TimePeriod=time_period, 
         Granularity=granularity,
         Metrics=metrics,
         GroupBy=groupBy
     )

    if response:
        results.extend(response['ResultsByTime'])
        while 'nextToken' in response:
            nextToken = response['nextToken']

            response = ce_client.get_cost_and_usage(
                 TimePeriod=time_period, 
                 Granularity=granularity,
                 Metrics=metrics,
                 GroupBy=groupBy,
                 NextPageToken=nextToken
            )
            results.extend(response['ResultsByTime'])
            if 'nextToken' in response:
                nextToken = response['nextToken']
            else:
                nextToken = False    
    return results


def writeToS3(bucket,key,data):
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(
                Body=json.dumps(data),
                Bucket=bucket,
                Key=key
            )
    except Exception as err:
        raise Exception(f"Could not upload to S3. {err}.")

    

def generateAutoStartEnd(granularity):
    today = datetime.datetime.today()
    if granularity == 'MONTHLY':
        firstDayOfThisMonth = today.replace(day=1)
        lastDayofLastMonth = firstDayOfThisMonth - datetime.timedelta(days=1)
        firstDayOfLastMonth = lastDayofLastMonth.replace(day=1)

        start = firstDayOfLastMonth.strftime(AWS_DATE_FORMAT)
        end = firstDayOfThisMonth.strftime(AWS_DATE_FORMAT)


        return start,end

    elif granularity == "DAILY":
        yesterday = today - datetime.timedelta(days=1).strftime(AWS_DATE_FORMAT)
        return yesterday,yesterday

    else:
        raise Exception("Parse error. 'granularity' should be one of 'MONTHLY'|'DAILY'")



def parseEvent(event):
    try:
        granularity = event["granularity"]
        st = event["start"]
        et = event["end"]
        if not st or not et:
            st , et = generateAutoStartEnd(granularity)
        metrics = event["metrics"]
        groupBy = event["groupBy"]
        return st, et, granularity, metrics, groupBy
    except Exception as err:
        raise Exception(f"Could not parse input, {err}")
        

def parseResults(results, metrics, primary_group, secondary_group):
    measurements = []
    for row in results:
        st = row['TimePeriod']['Start']
        for group in row['Groups']:
            primary = group['Keys'][0]
            secondary = group['Keys'][1]
            for metric in metrics:
                metric_value = float(group['Metrics'][metric]['Amount'])
                metric_unit = group['Metrics'][metric]['Unit']
                row_data = {
                        "timestamp": st,
                        "metric": metric,
                        "metric_value": metric_value,
                        "tags": [
                            {primary_group: primary},
                            {secondary_group:secondary},
                            {"metric_unit": metric_unit}
                        ]
                        
                    }
                measurements.append(row_data)
    return measurements

def opuleHandler(event, context):
    try:
        OPULENCE_S3 = os.environ["OPULENCE_S3"]
        MEASUREMENT = os.environ["MEASUREMENT"]
        st, et, granularity, metrics, groupBy = parseEvent(event)
        results = getCosts(st, et, granularity, metrics, groupBy)
        measurements = parseResults(results,metrics,groupBy[0]["Key"], groupBy[1]["Key"])
        data = {
            "name": MEASUREMENT,
            "measurements": measurements
        }
        fileName = st + "_" + et
        writeToS3(OPULENCE_S3,fileName, data)
        return {
            'statusCode': 200,
            'status': 'success',
            'errors': ""
        }


    except Exception as err:
        return {
            'statusCode': 400,
            'status': 'error',
            'errors': str(err)
        }


