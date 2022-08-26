class CostAndUsageRequest:
    def __init__(self, start:str, end: str, granularity: str, metrics: list, group_by: list):
        self.time_period = {'Start': start, 'End': end}
        self.granularity = granularity
        self.metrics = metrics
        self.group_by = group_by