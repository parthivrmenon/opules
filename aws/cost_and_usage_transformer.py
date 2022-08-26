from typing import List
from core.transformer import Transformer
from core.metric import Metric
from aws.cost_and_usage_response import CostAndUsageResponse
from aws.cost_and_usage_request import CostAndUsageRequest

class CustomTransformer(Transformer):

    def get_groups(self, request: CostAndUsageRequest) -> List:
        groups = []
        for group_def in request.group_by:
            groups.append(group_def['Key'])
        return groups

    def get_tags_from_line(self, line):
        pass 


    def transform(self, ce_request: CostAndUsageRequest, ce_response:CostAndUsageResponse) -> List[Metric]:
        metrics = []
        groups = self.get_groups(ce_request)
        for line in ce_response:
            timestamp = line['TimePeriod']['Start']
            for grp in line['Groups']:
                print(groups)
                print(grp)
                group_tags = []
                for idx,group in enumerate(groups):
                    tag_name = group
                    tag_value = grp['Keys'][idx]
                    group_tags.append({tag_name: tag_value})

                for metric in ce_request.metrics:
                    metric_value = grp['Metrics'][metric]['Amount']
                    metric_unit = grp['Metrics'][metric]['Unit']
                    m = Metric(name=metric, value=metric_value, timestamp=timestamp, tags=[{"UNIT":metric_unit}])
                    m.tags.extend(group_tags)
                    metrics.append(m)
                    print(m)
            print("x" * 100)
            
        return metrics


