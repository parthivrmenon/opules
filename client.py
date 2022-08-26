from core.influx_metric_store import InfluxMetricStore
from core.influx_metric_store import InfluxMetricStore
from aws.cost_and_usage_client import CostAndUsageApiClient
from aws.cost_and_usage_request import CostAndUsageRequest


def lambda_handler(event=None, context=None):
    # Prepare the request
    granularity="DAILY"
    start_time="2022-08-24"
    end_time="2022-08-26"
    metrics = ["UnblendedCost", "UsageQuantity"]
    group_by=[
            {"Type": "DIMENSION", "Key":"SERVICE"},
            {"Type": "DIMENSION", "Key":"REGION"}
            ]
    ce_request = CostAndUsageRequest(start_time,end_time,granularity,metrics,group_by)

    # call `fetch` method
    metrics = CostAndUsageApiClient(None, None).fetch(ce_request)

    # write received metrics to store.
    for metric in metrics:
        InfluxMetricStore().write(metric)


    
     




