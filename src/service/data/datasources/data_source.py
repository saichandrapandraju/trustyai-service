import logging
import os
import asyncio
from typing import List, Set, Dict, Optional, Any
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.service.data.model_data import ModelData
from src.service.data.storage import get_storage_interface
from src.service.constants import UNLABELED_TAG, GROUND_TRUTH_SUFFIX, METADATA_FILENAME, INTERNAL_DATA_FILENAME
from src.service.data.metadata.storage_metadata import StorageMetadata

logger = logging.getLogger(__name__)

class DataframeCreateException(Exception):
    """Exception raised when a dataframe cannot be created."""
    pass

#TODO: Create an abstract class and implement pandas df
class DataSource:
    METADATA_FILENAME = METADATA_FILENAME
    GROUND_TRUTH_SUFFIX = GROUND_TRUTH_SUFFIX
    INTERNAL_DATA_FILENAME = INTERNAL_DATA_FILENAME
    
    
    def __init__(self):
        self.known_models: Set[str] = set()
        self.storage_interface = get_storage_interface()
        self.metadata_cache: Dict[str, StorageMetadata] = {}
        self.executor = ThreadPoolExecutor(max_workers=10)

        
    # Dataframe operations
    def get_dataframe(self, model_id: str) -> pd.DataFrame:
        """
        Get a dataframe for the given model ID using the default batch size.
        
        Args:
            model_id: The model ID
            
        Returns:
            A pandas DataFrame with the model data
            
        Raises:
            DataframeCreateException: If the dataframe cannot be created
        """
        # Use default batch size from environment or config
        batch_size = int(os.environ.get("SERVICE_BATCH_SIZE", "100"))
        return self.get_dataframe_with_batch_size(model_id, batch_size)
    
    def get_dataframe_with_batch_size(self, model_id: str, batch_size: int) -> pd.DataFrame:
        """
        Get a dataframe for the given model ID with the specified batch size.
        
        Args:
            model_id: The model ID
            batch_size: The number of rows to include
            
        Returns:
            A pandas DataFrame with the model data
            
        Raises:
            DataframeCreateException: If the dataframe cannot be created
        """
        try:
            # Create model data object to access the data
            model_data = ModelData(model_id)
            
            # Get the total row count to determine how many rows to fetch
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Get row counts async
            input_rows, output_rows, metadata_rows = loop.run_until_complete(model_data.row_counts())
            
            # Get the minimum number of rows available
            available_rows = min(input_rows, output_rows, metadata_rows)
            
            # Calculate the start row to get the latest batch_size rows
            start_row = max(0, available_rows - batch_size)
            n_rows = min(batch_size, available_rows)
            
            # Get the data async
            input_data, output_data, metadata = loop.run_until_complete(
                model_data.data(start_row=start_row, n_rows=n_rows)
            )
            
            # Get column names async
            input_names, output_names, metadata_names = loop.run_until_complete(model_data.column_names())
            
            # Close the loop
            loop.close()
            
            # Combine the data into a single dataframe
            df_data = {}
            
            # Add input data
            for i, col_name in enumerate(input_names):
                if input_data is not None and i < input_data.shape[1]:
                    df_data[col_name] = input_data[:, i]
            
            # Add output data
            for i, col_name in enumerate(output_names):
                if output_data is not None and i < output_data.shape[1]:
                    df_data[col_name] = output_data[:, i]
            
            # Add metadata
            for i, col_name in enumerate(metadata_names):
                if metadata is not None and i < metadata.shape[1]:
                    df_data[col_name] = metadata[:, i]
            
            # Create dataframe
            return pd.DataFrame(df_data)
            
        except Exception as e:
            logger.error(f"Error creating dataframe for model={model_id}: {str(e)}")
            raise DataframeCreateException(f"Error creating dataframe for model={model_id}: {str(e)}")
    
    def get_organic_dataframe(self, model_id: str, batch_size: int) -> pd.DataFrame:
        """
        Get a dataframe with only organic data (not synthetic).
        
        Args:
            model_id: The model ID
            batch_size: The number of rows to include
            
        Returns:
            A pandas DataFrame with organic model data
            
        Raises:
            DataframeCreateException: If the dataframe cannot be created
        """
        df = self.get_dataframe_with_batch_size(model_id, batch_size)
        
        # Filter out any rows with the unlabeled tag
        if UNLABELED_TAG in df.columns:
            df = df[df[UNLABELED_TAG] != True]
        
        return df
    
    # Metadata operations
    def get_metadata(self, model_id: str) -> StorageMetadata:
        """
        Get metadata for the given model ID.
        
        Args:
            model_id: The model ID
            
        Returns:
            A StorageMetadata object
            
        Raises:
            Exception: If the metadata cannot be retrieved
        """
        if model_id in self.metadata_cache:
            return self.metadata_cache[model_id]
        
        try:
            # Create model data object
            model_data = ModelData(model_id)
            
            # Get row counts and column names async
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            input_rows, output_rows, metadata_rows = loop.run_until_complete(model_data.row_counts())
            input_names, output_names, metadata_names = loop.run_until_complete(model_data.column_names())
            
            # Close the loop
            loop.close()
            
            # Create metadata object
            metadata = StorageMetadata(model_id)
            metadata.observations = min(input_rows, output_rows, metadata_rows)
            
            # Check if there are inferences recorded (presence of UNLABELED_TAG in metadata)
            metadata.recorded_inferences = UNLABELED_TAG in metadata_names
            
            # Store in cache
            self.metadata_cache[model_id] = metadata
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata for model={model_id}: {str(e)}")
            raise Exception(f"Error getting metadata for model={model_id}: {str(e)}")
    
    def has_metadata(self, model_id: str) -> bool:
        """
        Check if metadata exists for the given model ID.
        
        Args:
            model_id: The model ID
            
        Returns:
            True if metadata exists, False otherwise
        """
        try:
            self.get_metadata(model_id)
            return True
        except Exception:
            return False
    
    # Model verification
    def get_verified_models(self) -> List[str]:
        """
        Get a list of verified model IDs (models that have data).
        
        Returns:
            A list of verified model IDs
        """
        verified_models = []
        
        # Check storage for all models
        # We need to get these from the storage interface
        models = self._get_all_model_names()
        
        for model_id in models:
            if self.has_metadata(model_id):
                verified_models.append(model_id)
                self.add_model_to_known(model_id)
        
        return verified_models
    
    def _get_all_model_names(self) -> List[str]:
        """
        Get all model names from storage.
        
        Returns:
            A list of model names
        """
        return list(self.known_models)
    
    def get_num_observations(self, model_id: str) -> int:
        """
        Get the number of observations for a model.
        
        Args:
            model_id: The model ID
            
        Returns:
            The number of observations
        """
        metadata = self.get_metadata(model_id)
        return metadata.observations
    
    def has_recorded_inferences(self, model_id: str) -> bool:
        """
        Check if inferences have been recorded for a model.
        
        Args:
            model_id: The model ID
            
        Returns:
            True if inferences have been recorded, False otherwise
        """
        metadata = self.get_metadata(model_id)
        return metadata.recorded_inferences
    
    # Ground truth operations
    @staticmethod
    def get_ground_truth_name(model_id: str) -> str:
        """
        Get the ground truth name for a model.
        
        Args:
            model_id: The model ID
            
        Returns:
            The ground truth name
        """
        return model_id + DataSource.GROUND_TRUTH_SUFFIX
    
    def has_ground_truths(self, model_id: str) -> bool:
        """
        Check if ground truths exist for a model.
        
        Args:
            model_id: The model ID
            
        Returns:
            True if ground truths exist, False otherwise
        """
        return self.has_metadata(self.get_ground_truth_name(model_id))