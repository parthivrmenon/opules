from typing import List
from aws.cost_and_usage_request import CostAndUsageRequest
from aws.cost_and_usage_response import CostAndUsageResponse
from aws.cost_and_usage_transformer import CustomTransformer
from core.metric import Metric

import boto3

class CostAndUsageApiClient:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.client = boto3.client(
            "ce",
            aws_access_key_id,
            aws_secret_access_key
        ) 

    # Not implemented yet.
    def fetch(self, request: CostAndUsageRequest) -> List[Metric]:
        metrics = CustomTransformer().transform(request, None)
        return metrics