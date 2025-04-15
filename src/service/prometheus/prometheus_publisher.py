import logging
import uuid
from typing import Dict, List, Optional, Any
from prometheus_client import Gauge, REGISTRY
from src.service.constants import PROMETHEUS_METRIC_PREFIX
from src.service.payloads.metrics.base_metric_request import BaseMetricRequest

logger = logging.getLogger(__name__)

class PrometheusPublisher:
    def __init__(self, registry = REGISTRY):
        self.registry = registry
        self.values: Dict[uuid.UUID, float] = {} #TODO: Implement AtomicDouble..?

    def _create_or_update_gauge(self, name: str, tags: Dict[str, str], id: uuid.UUID):
        existing_collector = self.registry._names_to_collectors.get(name)
        
        
        if existing_collector is None or not isinstance(existing_collector, Gauge):
            if existing_collector is not None:
                self.registry.unregister(existing_collector)
                logger.warning(f"Replaced non-Gauge collector {name} with a Gauge")
            
            gauge = Gauge(name=name,
                          documentation=f"TrustyAI metric: {name}",
                          labelnames=list(tags.keys()),
                          registry=self.registry,
                          )
        
        gauge.labels(**tags).set(self.values[id])
    
    def remove_gauge(self, name: str, id: uuid.UUID):
        full_name = f"{PROMETHEUS_METRIC_PREFIX}{name.lower()}"
        
        gauges_to_remove = []
        
        for collector in list(self.registry._names_to_collectors.values()):
            if collector.name == full_name and isinstance(collector, Gauge):
                gauge:Gauge = collector
                
                for labels, _ in gauge._metrics.items():
                    labels_dict = dict(zip(gauge._labelnames, labels))
                    if labels_dict.get("request") == str(id):
                        gauges_to_remove.append(gauge)
        
        # maybe do this without explicitly adding to a list and then unregistering..?
        for gauge in gauges_to_remove:
            self.registry.unregister(gauge)
        
        if id in self.values:
            del self.values[id]
        
    def _generate_tags(self, model_name:str, id:uuid.UUID, request: Optional[BaseMetricRequest] = None) -> Dict[str, str]:
        tags: Dict[str, str] = {}
        
        if request is not None:
            tags.update(request.retrieve_default_tags())
            tags.update(request.retrieve_tags())
        elif model_name:
            tags['model'] = model_name
        
        tags['request'] = str(id)
        return tags
    
    def gauge(self,
              model_name: str,
              id: uuid.UUID,
              request: Optional[BaseMetricRequest] = None,
              value: Optional[float] = None,
              named_values: Optional[Dict[str, float]] = None,
              metric_name: Optional[str] = None,
              ) -> None:
        """
        Register a gauge metric with multiple possible parameter combinations:
        - gauge(model_name, id, request, value)
        - gauge(model_name, id, request, named_values)
        - gauge(model_name, id, value, metric_name)
        """
        if request is not None:
            full_metric_name = f"{PROMETHEUS_METRIC_PREFIX}{request.metric_name.lower()}"
            
            if value is not None:
                self.values[id] = value
                tags = self._generate_tags(model_name=model_name,
                                        id=id,
                                        request=request)
                self._create_or_update_gauge(name=full_metric_name,
                                            tags=tags,
                                            id=id)
                logger.debug(f"Scheduled request for {request.metric_name} id={id}, value={value}")
                return
            
            if named_values is not None:
                for idx, (key, val) in enumerate(named_values.items()):
                    new_id = uuid.uuid5(uuid.NAMESPACE_DNS, (str(id)+str(idx)).encode("utf-8"))
                    self.values[new_id] = val
                    
                    tags = self._generate_tags(model_name=model_name,
                                               id=id,
                                               request=request)
                    tags["subcategory"] = key
                    self._create_or_update_gauge(name=full_metric_name,
                                                 tags=tags,
                                                 id=new_id)
                logger.debug(f"Scheduled request for {request.metric_name} id={id}, value={named_values}")
                return
        
        if metric_name is not None and value is not None:
            full_metric_name = f"{PROMETHEUS_METRIC_PREFIX}{metric_name.lower()}"
            
            self.values[id] = value
            tags = self._generate_tags(model_name=model_name, id=id)
            self._create_or_update_gauge(name=full_metric_name,
                                         tags=tags,
                                         id=id)
            
            logger.debug(f"Scheduled request for {metric_name} id={id}, value={value}")
            return
        