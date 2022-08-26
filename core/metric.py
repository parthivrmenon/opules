from dataclasses import dataclass

@dataclass
class Metric:
    name : str 
    value: str 
    timestamp: str 
    tags: list
    
     