from typing import List
from abc import ABC, abstractmethod
from core.metric import Metric

class Transformer(ABC):
    @abstractmethod
    def transform(self) -> List[Metric]:
        pass 
