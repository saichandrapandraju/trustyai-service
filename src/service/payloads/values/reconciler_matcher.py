
class ReconcilerMatcher:
    """
    Decorator class for fields that need reconciliation.
    Use to mark a field as requiring reconciliation with a specific type.
    """
    def __init__(self, name_provider: str):
        """
        Initialize with the name of the method that provides the field name.
        
        Args:
            name_provider: The name of the method that returns the field name
        """
        self.name_provider = name_provider
        
    def __call__(self, field_class):
        """Mark a field descriptor as needing reconciliation"""
        field_class._reconciler_matcher = self
        return field_class

# Utility function to create reconciler matcher field descriptors
def reconciler_field(field_type, name_provider: str):
    """
    Create a field descriptor that stores reconciler metadata
    """
    class ReconcilerFieldDescriptor:
        def __init__(self):
            self._reconciler_matcher = ReconcilerMatcher(name_provider)
            self._field_type = field_type
            self._name = None
            
        def __set_name__(self, name: str):
            self._name = f"_{name}"
            
        def __get__(self, instance):
            if instance is None:
                return self
            return getattr(instance, self._name, None)
            
        def __set__(self, instance, value):
            setattr(instance, self._name, value)
    
    return ReconcilerFieldDescriptor()