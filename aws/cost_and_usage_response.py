class CostAndUsageResponse:
    def __init__(self, groups: list, results: list, metrics: list):
        self.results = results 
        self.groups = groups 
        self.metrics = metrics