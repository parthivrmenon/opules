from core.metric_store import MetricStore
from core.metric import Metric
class InfluxMetricStore(MetricStore):
    def write(self, metric: Metric) -> bool:
        print(f"metric {metric} will be written to Influx.")
        return True


