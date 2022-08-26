from abc import ABC, abstractmethod
from core.metric import Metric

class MetricStore(ABC):
    @abstractmethod
    def write(self, metric: Metric) -> bool:
        pass 

