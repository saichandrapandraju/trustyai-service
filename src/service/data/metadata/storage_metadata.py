
from typing import Dict

class StorageMetadata:
    """Class representing storage metadata."""
    def __init__(self, model_id: str, observations: int = 0, recorded_inferences: bool = False, input_schema: dict = {}, output_schema: dict = {}, joint_name_aliases: Dict[str, str] = {}, input_tensor_name: str = "input", output_tensor_name: str = "output"):
        self.model_id = model_id
        self.observations = observations
        self.recorded_inferences = recorded_inferences
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.joint_name_aliases = joint_name_aliases
        self.input_tensor_name = input_tensor_name
        self.output_tensor_name = output_tensor_name