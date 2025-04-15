import logging
from typing import Dict, List, Optional, Any
import inspect

from src.service.payloads.metrics.base_metric_request import BaseMetricRequest
from src.service.data.datasources.data_source import DataSource

logger = logging.getLogger(__name__)

class RequestReconciler:
    @staticmethod
    def reconcile(request: BaseMetricRequest, data_source: DataSource) -> None:
        """
        Reconcile a metric request with the data source.
        
        Args:
            request: The metric request to reconcile
            data_source: The data source to use for reconciliation
        """
        try:
            # Get storage metadata for the model
            storage_metadata = data_source.get_metadata(request.model_id)
            
            # Get all attributes of the request object that are reconcilable
            for attr_name, attr_value in inspect.getmembers(request):
                # TODO: Here we would check if the attribute is a ReconcilableFeature or ReconcilableOutput
                # For now, we'll just log and assume it's already reconciled
                if hasattr(attr_value, 'reconcile'):
                    logger.debug(f"Reconciling {attr_name} in request for model {request.model_id}")
                    # Call reconcile method on the attribute
                    attr_value.reconcile(storage_metadata)
            
            logger.info(f"Reconciled request for model {request.model_id}")
        except Exception as e:
            logger.error(f"Error reconciling request: {e}")
            raise