import logging
import uuid
import os
from typing import Dict, Set

from fastapi import BackgroundTasks
import asyncio

from src.service.prometheus.prometheus_publisher import PrometheusPublisher
from src.service.payloads.metrics.base_metric_request import BaseMetricRequest
from src.endpoints.metrics.metrics_directory import MetricsDirectory
from src.service.payloads.metrics.request_reconciler import RequestReconciler
from src.service.data.datasources.data_source import DataSource

logger = logging.getLogger(__name__)

class PrometheusScheduler:

    def __init__(self, publisher: PrometheusPublisher = None, data_source: DataSource = None):
        self.requests: Dict[str, Dict[uuid.UUID, BaseMetricRequest]] = {}
        self.has_logged_skipped_request_message: Set[str] = set()
        self.metrics_directory = MetricsDirectory()
        self.publisher = publisher or PrometheusPublisher()
        self.data_source = data_source or DataSource()
        self.service_config = self._get_service_config()
        self._schedule_task_running = False

    def _get_service_config(self) -> Dict:
        """Get service configuration from environment variables."""
        return {
            "batch_size": int(os.getenv("SERVICE_BATCH_SIZE", "100")),
            "metrics_schedule": os.getenv("SERVICE_METRICS_SCHEDULE", "30s")
        }

    def schedule_calculation(self, background_tasks: BackgroundTasks):
        """Schedule the metric calculation as a background task."""
        if not self._schedule_task_running:
            background_tasks.add_task(self._periodic_calculation_task)
            self._schedule_task_running = True
            logger.info("Scheduled metric calculation as background task")

    async def _periodic_calculation_task(self):
        """Background task that periodically calculates metrics."""
        metrics_schedule = self.service_config.get("metrics_schedule", "30s")
        
        # Parse the schedule string to get the number of seconds
        if metrics_schedule.endswith("s"):
            interval = int(metrics_schedule[:-1])
        elif metrics_schedule.endswith("m"):
            interval = int(metrics_schedule[:-1]) * 60
        else:
            interval = 30  # Default to 30 seconds
        
        logger.info(f"Starting metrics scheduler with interval {interval}s")
        
        # This task will run indefinitely
        while True:
            try:
                await self.calculate_manual(False)
            except Exception as e:
                logger.error(f"Error in scheduler task: {e}")
            
            # Sleep for the specified interval
            await asyncio.sleep(interval)


    def get_requests(self, metric_name: str) -> Dict[uuid.UUID, BaseMetricRequest]:
        """Get all requests for a specific metric."""
        return dict(self.requests.get(metric_name, {}))

    def get_all_requests_flat(self) -> Dict[uuid.UUID, BaseMetricRequest]:
        """Get all requests across all metrics as a flat dictionary."""
        result = {}
        for metric_dict in self.requests.values():
            result.update(metric_dict)
        return result


    def calculate(self):
        """Calculate scheduled metrics."""
        self.calculate_manual(False)

    def calculate_manual(self, throw_errors: bool = True):
        """
        Calculate scheduled metrics.
        
        Args:
            throw_errors: If True, errors will be thrown. If False, they will just be logged.
        """
        try:
            # Get verified models
            verified_models = self.data_source.get_verified_models()
            
            # Global service statistic
            self.publisher.gauge(
                model_name="",
                id=uuid.uuid5(uuid.NAMESPACE_DNS, "model_count".encode("utf-8")),
                metric_name="MODEL_COUNT_TOTAL",
                value=len(verified_models)
            )
            
            requested_models = self.get_model_ids()
            
            for model_id in verified_models:
                # Global model statistics
                total_observations = self.data_source.get_num_observations(model_id)
                self.publisher.gauge(
                    model_name=model_id,
                    id=uuid.uuid5(uuid.NAMESPACE_DNS, model_id.encode("utf-8")),
                    metric_name="MODEL_OBSERVATIONS_TOTAL",
                    value=total_observations
                )
                
                has_recorded_inferences = self.data_source.has_recorded_inferences(model_id)
                
                if not has_recorded_inferences:
                    if model_id not in self.has_logged_skipped_request_message:
                        logger.info(f"Skipping metric calculation for model={model_id}, as no inference data has yet been recorded. Once inference data arrives, metric calculation will resume.")
                        self.has_logged_skipped_request_message.add(model_id)
                    continue
                
                if self.has_requests() and model_id in requested_models:
                    # Filter requests by model_id
                    requests_for_model = [
                        (req_id, request) for req_id, request in self.get_all_requests_flat().items()
                        if request.model_id == model_id
                    ]
                    
                    # Determine maximum batch requested
                    max_batch_size = max(
                        [request.batch_size for _, request in requests_for_model], 
                        default=self.service_config.get("batch_size", 100)
                    )
                    
                    # Get the dataframe with organic data
                    df = self.data_source.get_organic_dataframe(model_id, max_batch_size)
                    
                    for req_id, request in requests_for_model:
                        # Get batch for this request
                        batch_size = min(request.batch_size, df.shape[0])
                        batch = df.tail(batch_size)
                        
                        # Calculate the metric
                        metric_name = request.metric_name
                        calculator = self.metrics_directory.get_calculator(metric_name)
                        
                        if calculator:
                            value = calculator(batch, request)
                            
                            if value.is_single():
                                self.publisher.gauge(
                                    model_name=model_id,
                                    id=req_id,
                                    request=request,
                                    value=value.get_value()
                                )
                            else:
                                self.publisher.gauge(
                                    model_name=model_id,
                                    id=req_id,
                                    request=request,
                                    named_values=value.get_named_values()
                                )
                        else:
                            logger.warning(f"No calculator found for metric {metric_name}")
                    
        except Exception as e:
            if throw_errors:
                raise e
            else:
                logger.error(f"Error calculating metrics: {e}")

    def register(self, metric_name: str, id: uuid.UUID, request: BaseMetricRequest):
        """Register a metric request."""
        if metric_name not in self.requests:
            self.requests[metric_name] = {}
        
        # TODO: In a full implementation, we would reconcile the request here
        # For simplicity, added a stubbed one here.
        RequestReconciler.reconcile(request, self.data_source)
        
        self.requests[metric_name][id] = request
        logger.info(f"Registered request for metric {metric_name} with ID {id}")

    def delete(self, metric_name: str, id: uuid.UUID):
        """Delete a metric request."""
        if metric_name in self.requests and id in self.requests[metric_name]:
            del self.requests[metric_name][id]
            logger.info(f"Deleted request for metric {metric_name} with ID {id}")
        
        self.publisher.remove_gauge(metric_name, id)

    def has_requests(self) -> bool:
        """Check if there are any requests."""
        return any(bool(requests) for requests in self.requests.values())

    def get_model_ids(self) -> Set[str]:
        """Get unique model IDs with registered Prometheus metrics."""
        model_ids = set()
        for request in self.get_all_requests_flat().values():
            model_ids.add(request.model_id)
        return model_ids