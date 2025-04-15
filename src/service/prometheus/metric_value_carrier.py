from typing import Dict, Optional
from pydantic import BaseModel

class MetricValueCarrier(BaseModel):
    def __init__(self, value_or_named_values: float | Dict[str, float]):
        if isinstance(value_or_named_values, float):
            self.value: float = value_or_named_values
            self.named_values = None
            self.single: bool = True
        else:
            self.value = None
            self.named_values: Dict[str, float] = value_or_named_values
            self.single: bool = False
    
    def is_single(self) -> bool:
        return self.single
    
    def get_value(self) -> float:
        if self.single:
            return self.value
        else:
            raise ValueError("Metric value is not singular and therefore must be accessed via .get_named_values()")
    
    def get_named_values(self) -> Dict[str, float]:
        if not self.single:
            return self.named_values
        else:
            raise ValueError("Metric value is singular and therefore must be accessed via .get_value()")
    