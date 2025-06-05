from src.service.payloads.values.reconcilable_field import ReconcilableField

class ReconcilableFeature(ReconcilableField):
    """
    Class for reconcilable feature fields.
    """
    def __init__(self, raw_value_node):
        super().__init__(raw_value_node)